from yaml import load, dump

try:
    from yaml import CLoader as Loader, CDumper as Dumper
except ImportError:
    from yaml import Loader, Dumper

from fosse.utils import safeget


class Decoding:
    def __init__(
        self, regexp=None, date_group=None, time_group=None, name_group=None
    ):
        self.regexp = regexp
        self.date_group = date_group
        self.time_group = time_group
        self.name_group = name_group


class Notebook:
    def __init__(self, config, fosse_file=None):
        """
        Initializes the Notebook class.
        Args:
            config (dict): Configuration instance.
            fosse_file (str): Path to the fosse file.
        """
        self.config = config
        self.fosse_file = fosse_file
        self._fosse = None
        self._decoding = None
        if self.fosse_file:
            self.load_fosse(fosse_file)

    def __str__(self):
        """
        Returns a string representation of the Notebook.
        Returns:
            str: String representation of the Notebook.
        """
        s = "Notebook:\n"
        s = f"\tName: {safeget(self._fosse, 'name')}\n"
        if self._decoding:
            s += f"\tDecoding: {self._decoding.regexp}\n"
        return s

    def skip(self):
        """
        Returns whether the Notebook should be skipped.
        Returns:
            bool: True if the Notebook should be skipped, False otherwise.
        """
        return safeget(self._fosse, 'skip', False)

    def decoding(self):
        """
        Returns the decoding information for the Notebook.
        Returns:
            str: The decoding regexp for the Notebook.
        """
        if self._decoding:
            return self._decoding
        else:
            return None

    def name(self):
        """
        Returns the platform for the Notebook.
        Returns:
            str: The platform for the Notebook.
        """
        return safeget(self._fosse, 'name')

    def init_from_notebook(self, notebook):
        """
        Initializes the Notebook from another notebook.
        Args:
            notebook (dict): The notebook to initialize from.
        """
        self._fosse = notebook
        self._setup_decoding()

    def load_fosse(self, fosse_file):
        """
        Loads the fosse file.
        Args:
            fosse_file (str): Path to the fosse file.
        """
        with open(fosse_file, 'r') as file:
            self._fosse = load(file, Loader=Loader)
            self._setup_decoding()

    def _setup_decoding(self):
        self._decoding = Decoding(
            regexp=safeget(self._fosse, 'decoding', 'regexp'),
            date_group=safeget(self._fosse, 'decoding', 'date-group'),
            time_group=safeget(self._fosse, 'decoding', 'time-group'),
            name_group=safeget(self._fosse, 'decoding', 'name-group'),
        )

    def raw(self):
        """
        Returns a copy of the raw fosse data.
        Returns:
            dict: A copy of the raw fosse data.
        """
        if self._fosse is not None:
            return self._fosse.copy()

    def get_meta(self, key):
        """
        Gets the metadata for a given key.

        Args:
            key (str): The key to look up in the fosse file.
        Returns:
            str: The metadata associated with the key, or None if not found.

        NOTE:
            This function is dumb. It's intended to be used with the main
            metadata in the fosse yml file, and not anything that contains
            sub-arrays or sub-dictionaries. Think "platform" or "exclude".
            However, it will happily return whatever is held at key, whether
            it's a string or not.
        """
        if self._fosse is not None:
            return self._fosse.get(key, None)
