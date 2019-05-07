import time
from functools import wraps


def timing_val(func):
    @wraps(func)
    def wrapper(*arg, **kw):
        """Source: http://www.daniweb.com/code/snippet368.html"""
        t1 = time.time()
        res = func(*arg, **kw)
        t2 = time.time()
        return (t2 - t1), res, func.__name__

    return wrapper


def async_timing_val(func):
    @wraps(func)
    async def wrapper(*arg, **kw):
        """Source: http://www.daniweb.com/code/snippet368.html"""
        t1 = time.time()
        res = await func(*arg, **kw)
        t2 = time.time()
        return (t2 - t1), res, func.__name__

    return wrapper
