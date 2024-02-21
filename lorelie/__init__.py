import pathlib

PROJECT_PATH = pathlib.Path(__file__).parent.parent.absolute()


class BaseConnection:
    connection_settings = None
    configured = False

    def connect(self):
        pass


DATABASE = 'scraping'
