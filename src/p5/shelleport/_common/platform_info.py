#!/usr/bin/env python3
# -*- coding: utf-8 -*-

assert "__main__" != __name__


def _private():
    import os
    import sys

    _parent_package_name = ".".join(__package__.split(".")[:-1])

    class _Class(object):
        nt = "nt" == os.name
        tty = sys.stdin.isatty() and sys.stderr.isatty()
        program = f"{sys.executable} -m {_parent_package_name}" if ("__main__.py" == os.path.basename(sys.argv[0])) else None

    class _Result(object):
        Class = _Class

    return _Result


_private = _private()
try: Class = _private.Class
finally: del _private


# noinspection PyArgumentList
def make(*args, **kwargs): return Class(*args, **kwargs)
