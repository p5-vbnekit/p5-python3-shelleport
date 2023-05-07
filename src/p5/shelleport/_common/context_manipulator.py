#!/usr/bin/env python3
# -*- coding: utf-8 -*-

assert "__main__" != __name__


def _private():
    class _Class(object):
        @property
        def state(self): return self.__manager is not None

        @property
        def context(self):
            assert self.__manager is not None, "not opened"
            return self.__context

        @property
        def manager(self): return self.__manager

        @property
        def asynchronous(self): return self.__asynchronous

        @property
        def asynchronous_by_default(self): return self.__asynchronous_by_default

        def open(self, manager, asynchronous: bool = None):
            if asynchronous is None: asynchronous = self.__asynchronous_by_default
            else: assert isinstance(asynchronous, bool)
            assert self.__manager is None, "opened already"
            assert self.__context is None
            assert self.__asynchronous is None
            self.__asynchronous = asynchronous
            if asynchronous: return self.__async_open(manager = manager)
            return self.__sync_open(manager = manager)

        def close(self, asynchronous: bool = None, exception_info = None):
            if asynchronous is None: asynchronous = self.__asynchronous_by_default
            else: assert isinstance(asynchronous, bool)
            if exception_info is None: _exception_type, _exception_instance, _exception_traceback = None, None, None
            else: _exception_type, _exception_instance, _exception_traceback = exception_info
            _keywords = dict(
                exception_type = _exception_type,
                exception_instance = _exception_instance,
                exception_traceback = _exception_traceback
            )
            assert self.__manager is not None, "not opened"
            assert self.__asynchronous is asynchronous
            if asynchronous: return self.__async_close(**_keywords)
            self.__sync_close(**_keywords)

        def __init__(self, asynchronous: bool = False):
            assert isinstance(asynchronous, bool)
            super().__init__()
            self.__context = None
            self.__manager = None
            self.__asynchronous = None
            self.__asynchronous_by_default = asynchronous

        def __sync_open(self, manager):
            try: _context = manager.__enter__()
            except BaseException:
                self.__asynchronous = None
                raise

            self.__manager = manager
            self.__context = _context

            return _context

        async def __async_open(self, manager):
            try: _context = await manager.__aenter__()
            except BaseException:
                self.__asynchronous = None
                raise

            self.__manager = manager
            self.__context = _context

            return _context

        def __sync_close(self, exception_type, exception_instance, exception_traceback):
            _manager = self.__manager

            try:
                self.__context = None
                self.__manager = None

            finally:
                try: _manager.__exit__(exception_type, exception_instance, exception_traceback)
                finally: self.__asynchronous = None

        async def __async_close(self, exception_type, exception_instance, exception_traceback):
            _manager = self.__manager

            try:
                self.__context = None
                self.__manager = None

            finally:
                try: await _manager.__aexit__(exception_type, exception_instance, exception_traceback)
                finally: self.__asynchronous = None

    class _Result(object):
        Class = _Class

    return _Result


_private = _private()
try: Class = _private.Class
finally: del _private


# noinspection PyArgumentList
def make(*args, **kwargs): return Class(*args, **kwargs)
