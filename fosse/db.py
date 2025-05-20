import sqlite3
import os
import datetime
import sqlite3
import pickle
import json


class FosseData:
    def __init__(self, config):
        self.config = config

        self.db_file = config['db_file']

        self._con = sqlite3.connect(self.db_file)
        self.init_tables()

    def __del__(self):
        self._con.close()

    def init_tables(self):
        """
        Initializes the database tables.
        """
        cursor = self._con.cursor()

        # Create the Notebook table if it doesn't exist
        cursor.execute(
            '''
            CREATE TABLE IF NOT EXISTS notebooks (
                id INTEGER PRIMARY KEY,
                config_path TEXT NOT NULL UNIQUE,
                config_data TEXT NOT NULL,
                last_modified TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            '''
        )

        cursor.execute(
            '''
            CREATE INDEX IF NOT EXISTS idx_config_path ON notebooks(config_path)
            '''
        )

        cursor.execute(
            '''
            CREATE INDEX IF NOT EXISTS idx_last_modified ON notebooks(last_modified)
            '''
        )

        # Create normalized tables for shared metadata
        cursor.execute(
            '''
            CREATE TABLE IF NOT EXISTS genres (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL UNIQUE
            )
            '''
        )

        cursor.execute(
            '''
            CREATE TABLE IF NOT EXISTS subgenres (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL UNIQUE,
                genre_id INTEGER,
                FOREIGN KEY (genre_id) REFERENCES genres(id)
            )
            '''
        )

        cursor.execute(
            '''
            CREATE TABLE IF NOT EXISTS platforms (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL UNIQUE
            )
            '''
        )

        cursor.execute(
            '''
            CREATE TABLE IF NOT EXISTS titles (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL UNIQUE,
                platform_id INTEGER,
                FOREIGN KEY (platform_id) REFERENCES platforms(id)
            )
            '''
        )

        # Create the Video table if it doesn't exist
        cursor.execute(
            '''
            CREATE TABLE IF NOT EXISTS videos (
                id INTEGER PRIMARY KEY,
                file_path TEXT NOT NULL UNIQUE,
                file_data TEXT NOT NULL,
                last_modified TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_used TIMESTAMP,

                -- Video metadata
                duration_seconds INTEGER,
                width INTEGER,
                height INTEGER,
                video_format TEXT,
                codec TEXT,
                frame_rate REAL,
                file_size_bytes INTEGER,

                -- New metadata fields
                genre_id INTEGER,
                subgenre_id INTEGER,
                platform_id INTEGER,
                title_id INTEGER,
                recording_date TEXT,
                under_influence BOOLEAN DEFAULT 0,
                source_notebooks TEXT,

                -- Foreign key constraints
                FOREIGN KEY (genre_id) REFERENCES genres(id),
                FOREIGN KEY (subgenre_id) REFERENCES subgenres(id),
                FOREIGN KEY (platform_id) REFERENCES platforms(id),
                FOREIGN KEY (title_id) REFERENCES titles(id)
            )
            '''
        )

        # Create indexes for efficient searching - one statement per execute call
        cursor.execute(
            '''
            CREATE INDEX IF NOT EXISTS idx_file_path ON videos(file_path)
            '''
        )

        cursor.execute(
            '''
            CREATE INDEX IF NOT EXISTS idx_videos_last_used ON videos(last_used)
            '''
        )

        cursor.execute(
            '''
            CREATE INDEX IF NOT EXISTS idx_videos_format ON videos(video_format)
            '''
        )

        cursor.execute(
            '''
            CREATE INDEX IF NOT EXISTS idx_videos_duration ON videos(duration_seconds)
            '''
        )

        cursor.execute(
            '''
            CREATE INDEX IF NOT EXISTS idx_videos_resolution ON videos(width, height)
            '''
        )

        cursor.execute(
            '''
            CREATE INDEX IF NOT EXISTS idx_videos_genre ON videos(genre_id)
            '''
        )

        cursor.execute(
            '''
            CREATE INDEX IF NOT EXISTS idx_videos_subgenre ON videos(subgenre_id)
            '''
        )

        cursor.execute(
            '''
            CREATE INDEX IF NOT EXISTS idx_videos_platform ON videos(platform_id)
            '''
        )

        cursor.execute(
            '''
            CREATE INDEX IF NOT EXISTS idx_videos_title ON videos(title_id)
            '''
        )

        cursor.execute(
            '''
            CREATE INDEX IF NOT EXISTS idx_videos_recording_date ON videos(recording_date)
            '''
        )

        cursor.execute(
            '''
            CREATE INDEX IF NOT EXISTS idx_videos_influence ON videos(under_influence)
            '''
        )

        self._con.commit()

    def insert_video(self, file_path, metadata):
        """
        Inserts or updates a video in the database.

        Args:
            file_path (str): The path to the video file.
            metadata (dict): The metadata for the video.
        """
        # Extract normalized fields
        genre_name = metadata.get('genre')
        subgenre_name = metadata.get('subgenre')
        platform_name = metadata.get('platform')
        title_name = metadata.get('title')
        under_influence = metadata.get('under_influence', False)
        recording_date = metadata.get('recording_date')
        source_notebooks = metadata.get('source_notebooks', [])

        # Get or create IDs for normalized fields
        genre_id = self.get_or_create_genre(genre_name)
        platform_id = self.get_or_create_platform(platform_name)
        title_id = self.get_or_create_title(title_name, platform_id)
        subgenre_id = self.get_or_create_subgenre(subgenre_name, genre_id)

        # Serialize the full metadata for storage
        serialized_metadata = json.dumps(metadata)

        # Serialize source notebooks
        serialized_notebooks = json.dumps(source_notebooks)

        cursor = self._con.cursor()
        cursor.execute(
            """
            INSERT INTO videos (
                file_path, file_data, duration_seconds, width, height,
                video_format, codec, frame_rate, file_size_bytes,
                genre_id, subgenre_id, platform_id, title_id,
                recording_date, under_influence, source_notebooks
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(file_path) DO UPDATE SET
                file_data = excluded.file_data,
                duration_seconds = excluded.duration_seconds,
                width = excluded.width,
                height = excluded.height,
                video_format = excluded.video_format,
                codec = excluded.codec,
                frame_rate = excluded.frame_rate,
                file_size_bytes = excluded.file_size_bytes,
                genre_id = excluded.genre_id,
                subgenre_id = excluded.subgenre_id,
                platform_id = excluded.platform_id,
                title_id = excluded.title_id,
                recording_date = excluded.recording_date,
                under_influence = excluded.under_influence,
                source_notebooks = excluded.source_notebooks,
                last_modified = CURRENT_TIMESTAMP
            """,
            (
                file_path, serialized_metadata,
                metadata.get('duration_seconds', 0),
                metadata.get('width', 0),
                metadata.get('height', 0),
                metadata.get('video_format', 'unknown'),
                metadata.get('codec', 'unknown'),
                metadata.get('frame_rate', 0.0),
                metadata.get('file_size_bytes', 0),
                genre_id, subgenre_id, platform_id, title_id,
                recording_date, under_influence, serialized_notebooks
            )
        )
        self._con.commit()

    def insert_notebook(self, config_path, notebook):
        """
        Inserts or updates a Notebook into the database.
        Args:
            config_path (str): The path to the directory containing fosse.yml.
            config_data (str): The serialized content of the configuration (JSON/YAML).
        """
        config_data = pickle.dumps(notebook)
        cur = self._con.cursor()
        cur.execute(
            """
            INSERT INTO notebooks (config_path, config_data)
            VALUES (?, ?)
            ON CONFLICT(config_path) DO UPDATE SET
                config_data = excluded.config_data,
                last_modified = CURRENT_TIMESTAMP;
            """,
            (config_path, config_data),
        )
        cur.execute(
            """
            INSERT OR IGNORE INTO temp_existing_notebooks (path)
            VALUES (?);
            """,
            (config_path,),
        )
        self._con.commit()

    def get_applicable_notebook(self, file_path):
        """
        Retrieves the most specific Notebook for a given file path.
        Args:
            file_path (str): The file path for which to find the configuration.
        Returns:
            dict: The most specific Notebook.
        """
        # Get directory containing the file
        dir_path = os.path.dirname(file_path)
        parent_paths = []

        # Generate all possible parent directories
        while dir_path:
            parent_paths.append(dir_path)
            dir_path = os.path.dirname(dir_path)

        # Add root directory if needed
        if '/' not in parent_paths:
            parent_paths.append('/')

        cursor = self._con.cursor()

        # SQL query to find ALL configs that apply (not just most specific)
        placeholders = ','.join(['?'] * len(parent_paths))
        query = f"""
        SELECT config_path, config_data FROM notebooks
        WHERE config_path IN ({placeholders})
        ORDER BY LENGTH(config_path) ASC
        """

        cursor.execute(query, parent_paths)
        results = cursor.fetchall()

        # Merge configurations, with deeper directories taking precedence
        merged_config = {}
        for path, config_data in results:
            config = json.loads(config_data)
            # Update the merged config, overriding any existing keys
            merged_config.update(config)

        return merged_config

    def begin_of_scan(self):
        """
        To be called at the start of a scan. Will create temporary tables for
        videos and notebooks allowing for purging of entries that no longer
        exist.
        """
        self._con.execute("PRAGMA journal_mode = WAL")  # Better performance
        cursor = self._con.cursor()

        # Create temporary table for existing videos
        cursor.execute("DROP TABLE IF EXISTS temp_existing_files")
        cursor.execute("CREATE TEMPORARY TABLE temp_existing_files (path TEXT PRIMARY KEY)")

        # Create temporary table for existing notebooks
        cursor.execute("DROP TABLE IF EXISTS temp_existing_notebooks")
        cursor.execute("CREATE TEMPORARY TABLE temp_existing_notebooks (path TEXT PRIMARY KEY)")

        # Begin transaction
        self._con.execute("BEGIN TRANSACTION")

    def end_of_scan(self):
        """
        To be called at the end of a scan. Will purge entries that no longer
        exist in the filesystem.
        """
        cursor = self._con.cursor()

        # Delete videos not in temp_existing_files
        cursor.execute(
            """
            DELETE FROM videos
            WHERE file_path NOT IN (SELECT path FROM temp_existing_files)
            """
        )

        # Delete notebooks not in temp_existing_notebooks
        cursor.execute(
            """
            DELETE FROM notebooks
            WHERE config_path NOT IN (SELECT path FROM temp_existing_notebooks)
            """
        )

        # Commit transaction
        self._con.commit()

    def update_videos_for_config(self, config_path):
        """
        Updates all videos affected by changes to a config file.
        """
        cursor = self._con.cursor()

        # Find all video files in this directory and subdirectories
        like_pattern = f"{config_path}/%"

        # Get all videos in this directory and subdirectories
        cursor.execute("""
            SELECT id, file_path FROM videos
            WHERE file_path LIKE ? OR file_path = ?
        """, (like_pattern, config_path))

        affected_videos = cursor.fetchall()

        for video_id, file_path in affected_videos:
            # Get the combined configuration for this video
            combined_config = self.get_combined_config_for_file(file_path)

            # Extract metadata fields
            genre_name = combined_config.get('genre')
            subgenre_name = combined_config.get('subgenre')
            platform_name = combined_config.get('platform')
            title_name = combined_config.get('title')
            under_influence = combined_config.get('under_influence', False)

            # Get or create IDs for normalized fields
            genre_id = self.get_or_create_genre(genre_name)
            platform_id = self.get_or_create_platform(platform_name)
            title_id = self.get_or_create_title(title_name, platform_id)
            subgenre_id = self.get_or_create_subgenre(subgenre_name, genre_id)

            # Update the video record with the new configuration
            serialized_config = json.dumps(combined_config)
            cursor.execute("""
                UPDATE videos
                SET file_data = ?,
                    genre_id = ?,
                    subgenre_id = ?,
                    platform_id = ?,
                    title_id = ?,
                    under_influence = ?,
                    last_modified = CURRENT_TIMESTAMP
                WHERE id = ?
            """, (serialized_config, genre_id, subgenre_id, platform_id, title_id,
                under_influence, video_id))

        self._con.commit()

    def get_combined_config_for_file(self, file_path):
        """
        Calculates the combined configuration for a file by merging
        all applicable configs from parent directories.

        Args:
            file_path (str): Path to the video file

        Returns:
            dict: The combined configuration
        """
        # Get directory containing the file
        dir_path = os.path.dirname(file_path)
        parent_paths = []

        # Generate all possible parent directories
        while dir_path:
            parent_paths.append(dir_path)
            dir_path = os.path.dirname(dir_path)

        # Add root directory if needed
        if '/' not in parent_paths:
            parent_paths.append('/')

        cursor = self._con.cursor()

        # SQL query to find ALL configs that apply (not just most specific)
        placeholders = ','.join(['?'] * len(parent_paths))
        query = f"""
            SELECT config_path, config_data FROM notebooks
            WHERE config_path IN ({placeholders})
            ORDER BY LENGTH(config_path) DESC
        """

        cursor.execute(query, parent_paths)
        results = cursor.fetchall()

        # Track which notebooks contributed to this config
        source_notebooks = []

        # Merge configurations, with deeper directories taking precedence
        merged_config = {}
        for path, config_data in results:
            config = pickle.loads(config_data).raw()
            if config:
                # Update the merged config, overriding any existing keys
                merged_config.update(config)
                source_notebooks.append(path)

        # Add the source notebooks to the config
        merged_config['source_notebooks'] = source_notebooks

        return merged_config

    def get_combined_config_for_file(self, file_path):
        """
        Calculates the combined configuration for a file by merging
        all applicable configs from parent directories.

        Args:
            file_path (str): Path to the video file

        Returns:
            dict: The combined configuration
        """
        # Get directory containing the file
        dir_path = os.path.dirname(file_path)
        parent_paths = []

        # Generate all possible parent directories
        while dir_path:
            parent_paths.append(dir_path)
            dir_path = os.path.dirname(dir_path)

        # Add root directory if needed
        if '/' not in parent_paths:
            parent_paths.append('/')

        cursor = self._con.cursor()

        # Handle the case when there are no parent paths
        if not parent_paths:
            return {'source_notebooks': []}

        # SQL query to find ALL configs that apply (not just most specific)
        placeholders = ','.join(['?'] * len(parent_paths))
        query = f"""
        SELECT config_path, config_data FROM notebooks
        WHERE config_path IN ({placeholders})
        ORDER BY LENGTH(config_path) DESC
        """

        cursor.execute(query, parent_paths)
        results = cursor.fetchall()

        # Track which notebooks contributed to this config
        source_notebooks = []

        # Merge configurations, with deeper directories taking precedence
        merged_config = {}
        for path, config_data in results:
            config = pickle.loads(config_data).raw()
            if config:
                # Update the merged config, overriding any existing keys
                merged_config.update(config)
                source_notebooks.append(path)

        # Add the source notebooks to the config
        merged_config['source_notebooks'] = source_notebooks

        return merged_config

    def get_or_create_genre(self, genre_name):
        """
        Gets the ID for a genre, creating it if it doesn't exist.

        Args:
            genre_name (str): The name of the genre

        Returns:
            int: The ID of the genre
        """
        if not genre_name:
            return None

        cursor = self._con.cursor()
        cursor.execute("SELECT id FROM genres WHERE name = ?", (genre_name,))
        result = cursor.fetchone()

        if result:
            return result[0]

        cursor.execute("INSERT INTO genres (name) VALUES (?)", (genre_name,))
        self._con.commit()
        return cursor.lastrowid

    def get_or_create_platform(self, platform_name):
        """
        Gets the ID for a platform, creating it if it doesn't exist.

        Args:
            platform_name (str): The name of the platform

        Returns:
            int: The ID of the platform
        """
        if not platform_name:
            return None

        cursor = self._con.cursor()
        cursor.execute("SELECT id FROM platforms WHERE name = ?", (platform_name,))
        result = cursor.fetchone()

        if result:
            return result[0]

        cursor.execute("INSERT INTO platforms (name) VALUES (?)", (platform_name,))
        self._con.commit()
        return cursor.lastrowid

    def get_or_create_title(self, title_name, platform_id):
        """
        Gets the ID for a title, creating it if it doesn't exist.

        Args:
            title_name (str): The name of the title
            platform_id (int): The ID of the platform

        Returns:
            int: The ID of the title
        """
        if not title_name:
            return None

        cursor = self._con.cursor()
        cursor.execute("SELECT id FROM titles WHERE name = ?", (title_name,))
        result = cursor.fetchone()

        if result:
            return result[0]

        cursor.execute("INSERT INTO titles (name, platform_id) VALUES (?, ?)",
                    (title_name, platform_id))
        self._con.commit()
        return cursor.lastrowid

    def get_or_create_subgenre(self, subgenre_name, genre_id):
        """
        Gets the ID for a subgenre, creating it if it doesn't exist.

        Args:
            subgenre_name (str): The name of the subgenre
            genre_id (int): The ID of the parent genre

        Returns:
            int: The ID of the subgenre
        """
        if not subgenre_name:
            return None

        cursor = self._con.cursor()
        cursor.execute("SELECT id FROM subgenres WHERE name = ?", (subgenre_name,))
        result = cursor.fetchone()

        if result:
            return result[0]

        cursor.execute("INSERT INTO subgenres (name, genre_id) VALUES (?, ?)",
                    (subgenre_name, genre_id))
        self._con.commit()
        return cursor.lastrowid