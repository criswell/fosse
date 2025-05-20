import os
import datetime
import mimetypes
import pickle
import json
from pathlib import Path
from loguru import logger

from fosse.notebook import Notebook
from fosse.db import FosseData


class Scanner:
    def __init__(self, config):
        self.config = config
        self.db = FosseData(config)

    def handle_fosse_yml(self, dirpath):
        """
        Handles the fosse.yml file in the directory.

        Args:
            dirpath (str): The path of the directory being scanned.
        """
        notebook = Notebook(self.config, f"{dirpath}/{self.config['fosse_file']}")
        logger.debug(f"Found Notebook: {notebook.name()}")

        # Check if this is a new or updated notebook
        cursor = self.db._con.cursor()
        cursor.execute(
            "SELECT config_data FROM notebooks WHERE config_path = ?",
            (dirpath,)
        )
        existing_notebook = cursor.fetchone()

        is_updated = False
        if existing_notebook:
            old_notebook = pickle.loads(existing_notebook[0])
            # Compare old and new notebook data to see if it changed
            if old_notebook.raw() != notebook.raw():
                is_updated = True

        # Insert or update the notebook
        self.db.insert_notebook(dirpath, notebook)

        # If the notebook was updated, update all affected videos
        if is_updated:
            logger.info(f"Config updated at {dirpath}, updating affected videos...")
            self.db.update_videos_for_config(dirpath)

    def handle_video_file(self, dirpath, filename):
        """
        Handles a video file in the directory.

        Args:
            dirpath (str): The path of the directory containing the video file.
            filename (str): The name of the video file.
        """
        file_path = os.path.join(dirpath, filename)
        full_path = os.path.abspath(file_path)

        # Check if file exists in database and if it's been modified
        cursor = self.db._con.cursor()
        cursor.execute(
            "SELECT id, last_modified FROM videos WHERE file_path = ?",
            (full_path,)
        )
        result = cursor.fetchone()

        file_stat = os.stat(full_path)
        file_mtime = datetime.datetime.fromtimestamp(file_stat.st_mtime)

        # Mark this file as existing for end-of-scan cleanup
        cursor.execute(
            "INSERT OR IGNORE INTO temp_existing_files (path) VALUES (?)",
            (full_path,)
        )

        # If file doesn't exist in DB or has been modified, process it
        if not result or (result and file_mtime > datetime.datetime.fromisoformat(result[1])):
            logger.info(f"Processing video file: {full_path}")

            # Get video metadata
            metadata = self.extract_video_metadata(full_path)

            # Get combined configuration for this file
            config_data = self.db.get_combined_config_for_file(full_path)

            # Extract recording date from filename if possible
            recording_date = self.extract_recording_date(filename, dirpath)

            # Combine all metadata
            combined_metadata = {
                'file_path': full_path,
                'file_size_bytes': file_stat.st_size,
                'recording_date': recording_date,
                **metadata,
                **config_data
            }

            # Insert or update the video in the database
            self.db.insert_video(full_path, combined_metadata)

            logger.debug(f"Added/updated video: {filename}")

    def extract_video_metadata(self, file_path):
        """
        Extracts metadata from a video file using pymediainfo.

        Args:
            file_path (str): Path to the video file.

        Returns:
            dict: Metadata extracted from the video file.
        """
        try:
            from pymediainfo import MediaInfo

            logger.debug(f"Extracting metadata for: {file_path}")

            media_info = MediaInfo.parse(file_path)

            # Get the video track (usually the first video track)
            video_track = None
            for track in media_info.tracks:
                if track.track_type == 'Video':
                    video_track = track
                    break

            if not video_track:
                logger.warning(f"No video track found in {file_path}")
                return self._get_default_metadata()
            else:
                # Log all available attributes for debugging
                logger.debug(f"Available video track attributes: {dir(video_track)}")
                for attr in dir(video_track):
                    if not attr.startswith('_'):  # Skip private attributes
                        try:
                            value = getattr(video_track, attr)
                            if not callable(value):  # Skip methods
                                logger.debug(f"  {attr}: {value}")
                        except Exception:
                            pass


            # Helper function to safely get numeric attributes
            def safe_int(obj, attr, default=0):
                if hasattr(obj, attr) and getattr(obj, attr) is not None:
                    try:
                        return int(float(getattr(obj, attr)))
                    except (ValueError, TypeError):
                        logger.debug(f"Could not convert {attr} to int: {getattr(obj, attr)}")
                        return default
                return default

            def safe_float(obj, attr, default=0.0):
                if hasattr(obj, attr) and getattr(obj, attr) is not None:
                    try:
                        return float(getattr(obj, attr))
                    except (ValueError, TypeError):
                        logger.debug(f"Could not convert {attr} to float: {getattr(obj, attr)}")
                        return default
                return default

            # Extract relevant metadata with safer conversions
            metadata = {
                'duration_seconds': safe_int(video_track, 'duration', 0) // 1000,  # Convert ms to seconds
                'width': safe_int(video_track, 'width', 0),
                'height': safe_int(video_track, 'height', 0),
                'video_format': getattr(video_track, 'format', 'unknown') if hasattr(video_track, 'format') else 'unknown',
                'codec': getattr(video_track, 'codec_id', 'unknown') if hasattr(video_track, 'codec_id') else 'unknown',
                'frame_rate': safe_float(video_track, 'frame_rate', 0.0),
                'bit_rate': safe_int(video_track, 'bit_rate', 0),
                'aspect_ratio': getattr(video_track, 'display_aspect_ratio', None) if hasattr(video_track, 'display_aspect_ratio') else None,
            }

            return metadata

        except ImportError:
            logger.warning("pymediainfo not installed. Using default metadata values.")
            return self._get_default_metadata()
        except Exception as e:
            logger.error(f"Error extracting metadata from {file_path}: {str(e)}")
            logger.debug(f"Exception details:", exc_info=True)  # Add full traceback for debugging
            return self._get_default_metadata()

    def _get_default_metadata(self):
        """
        Returns default metadata when extraction fails.

        Returns:
            dict: Default metadata values.
        """
        return {
            'duration_seconds': 0,
            'width': 0,
            'height': 0,
            'video_format': 'unknown',
            'codec': 'unknown',
            'frame_rate': 0.0,
            'bit_rate': 0,
            'aspect_ratio': None,
        }

    def extract_recording_date(self, filename, dirpath):
        """
        Attempts to extract recording date from filename using notebook decoding rules.
        """
        import re
        from datetime import datetime

        # Get combined config for this file path
        file_path = os.path.join(dirpath, filename)
        combined_config = self.db.get_combined_config_for_file(file_path)

        # Check if we have decoding information
        if not combined_config or 'decoding' not in combined_config:
            return None

        decoding = combined_config.get('decoding', {})
        regexp = decoding.get('regexp')
        date_group = decoding.get('date-group')
        time_group = decoding.get('time-group')

        if not regexp or not date_group:
            return None

        try:
            # Apply the regular expression to the filename
            match = re.match(regexp, filename)
            if not match:
                return None

            # Extract date and time if available
            date_str = match.group(date_group)
            time_str = match.group(time_group) if time_group and time_group <= len(match.groups()) else "00:00:00"

            # Parse date format - this would need to be adjusted based on your date format
            # Assuming format is YYYY-MM-DD or similar
            date_format = combined_config.get('date_format', '%Y-%m-%d')
            time_format = combined_config.get('time_format', '%H:%M:%S')

            # Try to parse the date
            try:
                recording_date = datetime.strptime(f"{date_str} {time_str}", f"{date_format} {time_format}")
                return recording_date.isoformat()
            except ValueError:
                logger.warning(f"Could not parse date from filename: {filename}")
                return None

        except Exception as e:
            logger.error(f"Error extracting date from {filename}: {str(e)}")
            return None

    def scan(self):
        """
        Scans the directory specified in the config for video files. Populates
        the database.

        Returns:
            bool: True if the scan was successful, False otherwise.
        """
        logger.info("Starting scan...")
        self.db.begin_of_scan()

        # Initialize mimetypes
        mimetypes.init()

        # Define video extensions to look for
        video_extensions = self.config['video_extensions']

        fosse_file = "fosse.yml"  # Default fosse file name
        if 'fosse_file' in self.config:
            fosse_file = self.config['fosse_file']

        # Convert to Path object for better path handling
        root = Path(self.config['root'])

        # Check if the root path exists
        if not root.exists():
            logger.error(
                f"Error: Path '{root}' does not exist. Please check your configuration."
            )
            return False

        # Walk through all directories and files
        for dirpath, dirnames, filenames in os.walk(root):
            logger.debug(f"Scanning {dirpath}...")

            # Handle fosse.yml file if it exists
            if fosse_file in filenames:
                self.handle_fosse_yml(dirpath)

            # Handle video files
            for filename in filenames:
                # Check if file has a video extension
                if any(filename.lower().endswith(ext) for ext in video_extensions):
                    self.handle_video_file(dirpath, filename)

        # Clean up database entries for files that no longer exist
        self.db.end_of_scan()
        logger.info("Scan completed successfully")

        return True