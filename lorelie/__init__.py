import logging
import pathlib
from collections import defaultdict, deque

PROJECT_PATH = pathlib.Path(__file__).parent.parent.absolute()

DATABASE = 'scraping'


# __all__ = [
#     'Database',
#     'Table',
#     'fields'
# ]


class LogQueries:
    """Class which can be used to log queries
    made by the database. This is generally for
    debugging or monitoring purposes
    """
    container = deque()
    by_table = defaultdict(deque)

    def __init__(self, maxsize=100):
        self.maxsize = maxsize

    def __repr__(self):
        return f'<{self.__class__.__name__}: {len(self.container)}>'

    def __iter__(self):
        return iter(self.container)

    def __len__(self):
        return len(self.container)

    def append(self, statement, table=None, backend=None):
        self.container.append(statement)

        if backend is not None:
            table = getattr(backend, 'current_table')

        if table is not None:
            try:
                container = self.by_table[table.name]
            except:
                pass
            else:
                container.append(statement)

        if len(self.container) > self.maxsize:
            self.container.clear()


log_queries = LogQueries()


class LorelieLogger:
    def __init__(self):
        logger = logging.getLogger('lorelie')

        handler = logging.FileHandler('queries.log')
        logger.addHandler(handler)
        logger.setLevel(logging.DEBUG)

        log_format = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s: %(message)s',
            datefmt='%Y-%m-%d %H:%M'
        )
        handler.setFormatter(log_format)

        self.logger = logger
    
    def debug(self, message, *args, **kwargs):
        self.logger.debug(message, *args, **kwargs)

    def info(self, message, *args, **kwargs):
        self.logger.info(message, *args, **kwargs)

    def warning(self, message, *args, **kwargs):
        self.logger.warning(message, *args, **kwargs)


lorelie_logger = LorelieLogger()
