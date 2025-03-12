import click

from loguru import logger
from fosse.config import Config
from fosse.scanner import Scanner


def list_commands(config):
    print("Available commands:\n")
    for command in COMMANDS.keys():
        print(f"'{command}'- {COMMANDS[command]['desc']}")
        print(f"{' '*4}{COMMANDS[command]['details']}\n")


def scan(config):
    """
    Scans the configured root directory for video files and stores or updates the results in the database.
    """
    scanner = Scanner(config)
    scanner.scan()


def init(config):
    pass


def unimplemented(config):
    print("This command is not yet implemented.")


COMMANDS = {
    'list': {
        'desc': 'List commands',
        'details': 'Lists the available commands and their descriptions.',
        'func': list_commands,
    },
    'scan': {
        'desc': 'Scan video files',
        'details': 'Scans the configured root directory for video files and stores or updates the results in the database.',
        'func': scan,
    },
    'stream': {
        'desc': 'Stream video files',
        'details': 'Starts the video stream.',
        'func': unimplemented,
    },
    'check': {
        'desc': 'Check setup',
        'details': 'Checks your setup and configuration for issues.',
        'func': unimplemented,
    },
    'init': {
        'desc': 'Initialize the database',
        'details': 'Initializes the database. Operation should be idempotent.',
        'func': init,
    },
}


@click.command()
@click.option(
    '--config', '-c', default='fosse-config.yml', help='Path to config file.'
)
@click.argument('command')
def cli(config, command):
    """
    The Bob Fosse of video streaming.

    \b
    Commands:
        list   - List commands
        scan   - Scan video files
        stream - Stream video files
        check  - Check setup
    """
    config = Config(config)

    if 'log_file' in config:
        logger.add(
            config['log_file'],
            rotation='1 MB',
            compression='zip',
        )

    if command not in COMMANDS.keys():
        print(f"Error: Command '{command}' not found.")
        list_commands()
        return

    COMMANDS[command]['func'](config)
