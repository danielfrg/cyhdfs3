"""
Taken from: https://gist.github.com/acdha/4068406
"""

import sys
import inspect
from timeit import default_timer


class Timer(object):
    """Context Manager to simplify timing Python code
    Usage:
        with Timer('key step'):
            ... do something ...
    """
    def __init__(self, context=None, summary=True):
        self.timer = default_timer
        self.summary = summary

        if context is None:
            caller_frame = inspect.stack()[1]
            frame = caller_frame[0]
            info = inspect.getframeinfo(frame)

            context = '%s (%s:%s)' % (info.function, info.filename, info.lineno)

        self.context = context

    @property
    def elapsed(self):
        end = self.timer()
        return (end - self.start) * 1000

    def restart(self):
        self.start = self.timer()

    def __enter__(self):
        self.start = self.timer()
        return self

    def __exit__(self, *args):
        if self.summary:
            print >>sys.stderr, '%s: %f ms' % (self.context, self.elapsed)
