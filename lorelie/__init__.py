import logging
import logging.config
import pathlib
from collections import defaultdict, deque
from typing import Optional

from lorelie.lorelie_typings import TypeSQLiteBackend, TypeTable

PROJECT_PATH = pathlib.Path(__file__).parent.parent.absolute()


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
    container: deque[str] = deque()
    by_table: defaultdict[str, deque[str]] = defaultdict(deque)
    mask_values: bool = False

    def __init__(self, maxsize=100):
        self.maxsize = maxsize

    def __repr__(self):
        return f'<{self.__class__.__name__}: {len(self.container)}>'

    def __iter__(self):
        yield self.container[-1]

    def __len__(self):
        return len(self.container)

    def append(self, statement: str, table: Optional[TypeTable] = None, backend: Optional[TypeSQLiteBackend] = None):
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

                if len(container) > self.maxsize:
                    self.container = container[-self.maxsize - 1:]

        if len(self.container) > self.maxsize:
            self.container.clear()


log_queries = LogQueries()


log_config = {
    'version': 1,
    'formatters': {
        'standard': {
            'format': '%(asctime)s - %(name)s - %(levelname)s: %(message)s',
            'datefmt': '%Y-%m-%d %H:%M'
        }
    },
    'handlers': {
        'stdout_handler': {
            'class': 'logging.StreamHandler',
            'formatter': 'standard',
            'level': 'DEBUG',
        },
        'file_handler': {
            'class': 'logging.FileHandler',
            'filename': 'queries.log',
            'formatter': 'standard',
            'level': 'DEBUG',
        }
    },
    'loggers': {
        'lorelie': {
            'handlers': ['file_handler', 'stdout_handler'],
            'level': 'DEBUG',
            'propagate': False,
        }
    }
}


# class LorelieLogger:
#     def __init__(self):
#         logger = logging.getLogger('lorelie')

#         handler = logging.FileHandler('queries.log')
#         logger.addHandler(handler)
#         logger.setLevel(logging.DEBUG)

#         log_format = logging.Formatter(
#             '%(asctime)s - %(name)s - %(levelname)s: %(message)s',
#             datefmt='%Y-%m-%d %H:%M'
#         )
#         handler.setFormatter(log_format)

#         self.logger = logger

#     def debug(self, message, *args, **kwargs):
#         self.logger.debug(message, *args, **kwargs)

#     def info(self, message, *args, **kwargs):
#         self.logger.info(message, *args, **kwargs)

#     def warning(self, message, *args, **kwargs):
#         self.logger.warning(message, *args, **kwargs)

def get_logger(name: str = 'lorelie') -> logging.Logger:
    logging.config.dictConfig(log_config)
    return logging.getLogger(name)


lorelie_logger = get_logger()
