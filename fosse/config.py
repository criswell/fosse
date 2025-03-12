from yaml import load

try:
    from yaml import CLoader as Loader
except ImportError:
    from yaml import Loader


class Config:
    def __init__(self, config_file):
        with open(config_file, 'r') as file:
            self._config = load(file, Loader=Loader)

    def __getitem__(self, key):
        return self._config[key]

    def keys(self):
        return self._config.keys()

    def items(self):
        return self._config.items()

    def __repr__(self):
        return repr(self._config)

    def __str__(self):
        return str(self._config)

    def __contains__(self, item):
        return item in self._config
