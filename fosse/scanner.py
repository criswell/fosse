import os
import datetime
import mimetypes
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
        Handles the .fosse.yml file in the directory.

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
        pass

    def scan(self):
        """
        Scans the directory specified in the config for video files. Populates
        the database.

        Returns:
            bool: True if the scan was successful, False otherwise.
        """
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

        video_files = {}

        # Walk through all directories and files
        for dirpath, dirnames, filenames in os.walk(root):
            logger.debug(f"Scanning {dirpath}...")
            if fosse_file in filenames:
                self.handle_fosse_yml(dirpath)
            for filename in filenames:
                # Check if file has a video extension
                if filename.lower().endswith(video_extensions):
                    self.handle_video_file(dirpath, filename)
                #    print(f"{dirpath} : {filename}")
                #    if dirpath not in video_files.keys():
                #        video_files[dirpath] = []

                #    file_path = Path(dirpath) / filename
                #    file_info = {
                #        'name': filename,
                #        'size': os.path.getsize(file_path),
                #        'creation_time': datetime.datetime.fromtimestamp(
                #            os.path.getctime(file_path)
                #        ),
                #        'modification_time': datetime.datetime.fromtimestamp(
                #            os.path.getmtime(file_path)
                #        ),
                #        'mime_type': mimetypes.guess_type(file_path)[0],
                #    }
                #    video_files[dirpath].append(file_info)
                pass

        return True
