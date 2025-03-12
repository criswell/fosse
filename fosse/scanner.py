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
        Handles the .fosse.yml file in the directory. This function is a placeholder
        and should be implemented based on the specific requirements of your application.

        Args:
            dirpath (str): The path of the directory being scanned.
        """
        notebook = Notebook(
            self.config, f"{dirpath}/{self.config['fosse_file']}"
        )
        logger.debug(f"Found Notebook: {notebook.name()}")
        self.db.insert_notebook(dirpath, notebook)

    def scan(self):
        """
        Scans the directory specified in the config for video files. Populates
        the database.

        Returns:
            bool: True if the scan was successful, False otherwise.
        """
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
                self.handle_fosse_yml(dirpath, dirnames)
            for filename in filenames:
                # Check if file has a video extension
                # if filename.lower().endswith(video_extensions):
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
