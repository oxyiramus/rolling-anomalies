import time

import structlog

logger = structlog.get_logger()


def track(foo_func):
    """Decorator for logging function execution time."""
    def track_time():
        start_time = time.perf_counter()
        foo_func()
        end_time = time.perf_counter()
        total_time = end_time - start_time
        logger.debug(f'Time to execute `{foo_func.__name__}`: {total_time:.3f} s.')
    return track_time
