"""
Taken from: https://gist.github.com/acdha/4068406
"""

class Timer(object):
    """Context Manager to simplify timing Python code
    Usage:
        with Timer('key step'):
            ... do something ...
    """
    def __init__(self, context=None):
        from timeit import default_timer
        self.timer = default_timer

        if context is None:
            import inspect
            caller_frame = inspect.stack()[1]
            frame = caller_frame[0]
            info = inspect.getframeinfo(frame)

            context = '%s (%s:%s)' % (info.function, info.filename, info.lineno)

        self.context = context
    def __enter__(self):
        self.start = self.timer()
        return self

    def __exit__(self, *args):
        end = self.timer()

        elapsed = (end - self.start) * 1000

        import sys
        print >>sys.stderr, '%s: %f ms' % (self.context, elapsed)
