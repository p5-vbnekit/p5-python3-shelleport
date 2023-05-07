#!/usr/bin/env python3
# -*- coding: utf-8 -*-

assert "__main__" != __name__


def _private():
    import asyncio
    import contextlib

    async def _cleanup(subprocess: asyncio.subprocess.Process):
        if subprocess.returncode is not None: return
        try:
            subprocess.terminate()
            await asyncio.wait_for(subprocess.wait(), timeout = +3.0e+0)
        finally:
            if subprocess.returncode is None: subprocess.kill()

    @contextlib.asynccontextmanager
    async def _open(subprocess: asyncio.subprocess.Process):
        assert isinstance(subprocess, asyncio.subprocess.Process)
        try: yield
        finally: await asyncio.shield(_cleanup(subprocess = subprocess))

    class _Class(object):
        @property
        def subprocess(self): return self.__subprocess

        async def open(self):
            _context = self.__context
            assert _context is None
            _context = _open(subprocess = self.__subprocess)
            await _context.__aenter__()
            self.__context = _context

        async def close(self):
            _context = self.__context
            assert _context is not None
            self.__context = None
            await _context.__aexit__(None, None, None)

        async def __aenter__(self): return self

        async def __aexit__(self, exception_type, exception_instance, exception_traceback):
            _context = self.__context
            if _context is None: return
            self.__context = None
            await _context.__aexit__(exception_type, exception_instance, exception_traceback)

        def __init__(self, subprocess: asyncio.subprocess.Process):
            super().__init__()
            assert isinstance(subprocess, asyncio.subprocess.Process)
            self.__context = None
            self.__subprocess = subprocess

    class _Result(object):
        Class = _Class

    return _Result


_private = _private()
try: Class = _private.Class
finally: del _private


# noinspection PyArgumentList
def make(*args, **kwargs): return Class(*args, **kwargs)
