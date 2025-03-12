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

        self._con.commit()
        self._con.close()

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
        self._con.close()

        # Merge configurations, with deeper directories taking precedence
        merged_config = {}
        for path, config_data in results:
            config = json.loads(config_data)
            # Update the merged config, overriding any existing keys
            merged_config.update(config)

        return merged_config
