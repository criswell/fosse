from setuptools import setup, find_packages

_globals = {}
with open("fosse/__version__.py") as f:
    exec(f.read(), _globals)

setup(
    name='fosse',
    description='The Bob Fosse of automated video streaming',
    version=_globals['__version__'],
    author='Sam Hart',
    author_email='hartsn@gmail.com',
    url='foo.com',
    packages=find_packages(),
    install_requires=[
        'click',
        'pyyaml',
        'loguru',
        'pymediainfo',
    ],
    entry_points={
        'console_scripts': [
            'fosse = fosse.cli:cli',
        ],
    },
    include_package_data=True,
)
