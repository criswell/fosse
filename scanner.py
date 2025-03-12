#!/usr/bin/env python

import os
import datetime
import mimetypes
from pathlib import Path

import click

from .fosse.config import Config


def find_video_files(root_path):
    """
    Recursively search for video files in the given root path and its subdirectories.

    Args:
        root_path (str): The root directory to start searching from

    Returns:
        dict: A dictionary where keys are file paths and values are dictionaries containing
              file information such as name, size, creation time, etc.
    """
    # Initialize mimetypes
    mimetypes.init()

    # Define video extensions to look for
    video_extensions = (
        '.mp4',
        '.mkv',
        '.webm',
        '.avi',
        '.mov',
        '.flv',
        '.wmv',
        '.m4v',
    )

    # Dictionary to store results
    video_files = {}

    # Convert to Path object for better path handling
    root = Path(root_path)

    # Check if the root path exists
    if not root.exists():
        print(f"Error: Path '{root}' does not exist.")
        return video_files

    # Walk through all directories and files
    for dirpath, dirnames, filenames in os.walk(root):
        if '.fosse.yml' in filenames:
            pass
        for filename in filenames:
            # Check if file has a video extension
            if filename.lower().endswith(video_extensions):
                print(f"{dirpath} : {filename}")
                if dirpath not in video_files.keys():
                    video_files[dirpath] = []

                file_path = Path(dirpath) / filename

                # Get file stats
                stats = file_path.stat()

                # Try to get MIME type
                mime_type, _ = mimetypes.guess_type(file_path)

                # Store file information
                #video_files[full_path] = {
                #    'name': filename,
                #    'directory': str(Path(dirpath)),
                #    'size_bytes': stats.st_size,
                #    'size_mb': round(stats.st_size / (1024 * 1024), 2),
                #    'created': datetime.datetime.fromtimestamp(
                #        stats.st_ctime
                #    ).strftime('%Y-%m-%d %H:%M:%S'),
                #    'modified': datetime.datetime.fromtimestamp(
                #        stats.st_mtime
                #    ).strftime('%Y-%m-%d %H:%M:%S'),
                #    'extension': Path(filename).suffix.lower(),
                #    'mime_type': mime_type if mime_type else 'Unknown',
                #    'relative_path': (
                #        str(file_path.relative_to(root))
                #        if file_path.is_relative_to(root)
                #        else None
                #    ),
                #}

    print(f"Found {len(video_files)} video files.")
    return video_files


@click.command()
@click.option(
    '--config', '-c', default='config.yaml', help='Path to config file.'
)
@click.argument('command', type=click.Path(exists=True))
def shy_guy_tries_french_fries(config, command):
    """Shy Guy tries French Fries."""
    print(config)

    #raw_files = find_video_files(path)

    #print(raw_files)


if __name__ == '__main__':
    shy_guy_tries_french_fries()
