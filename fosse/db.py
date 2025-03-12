import sqlite3
import os
import datetime
import sqlite3
import pickle


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
        )
        '''
        )

        cursor.execute(
            '''
            CREATE INDEX IF NOT EXISTS idx_file_path ON videos(file_path);
            -- Index for last_used timestamp searches
            CREATE INDEX IF NOT EXISTS idx_videos_last_used ON videos(last_used);

            -- Indexes for common metadata searches
            CREATE INDEX IF NOT EXISTS idx_videos_format ON videos(video_format);
            CREATE INDEX IF NOT EXISTS idx_videos_duration ON videos(duration_seconds);
            CREATE INDEX IF NOT EXISTS idx_videos_resolution ON videos(width, height);
            '''
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
        self._con.commit()

    def get_applicable_notebook(file_path):
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
        conn.execute("BEGIN TRANSACTION")
