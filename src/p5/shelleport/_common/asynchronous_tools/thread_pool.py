#!/usr/bin/env python3
# -*- coding: utf-8 -*-

assert "__main__" != __name__


def _private():
    import asyncio
    import contextlib
    import concurrent.futures

    from .. import context_manipulator as _context_manipulator_module

    _make_context_manipulator = _context_manipulator_module.make

    @contextlib.contextmanager
    def _open_context(arguments, keywords):
        with concurrent.futures.ThreadPoolExecutor(*arguments, **keywords) as _executor: yield _executor

    class _Class(object):
        def open(self): self.__manipulator.open(manager = _open_context(
            arguments = self.__arguments, keywords = self.__keywords
        ))

        def close(self): self.__manipulator.close(exception_info = None)

        async def __call__(self, *args, **kwargs):
            _executor = self.__manipulator.context
            assert _executor is not None
            _loop = asyncio.get_running_loop()
            args = list(args)
            _delegate = args.pop(0)
            assert isinstance(_loop, asyncio.AbstractEventLoop)
            return await _loop.run_in_executor(_executor, lambda: _delegate(*args, **kwargs))

        def __enter__(self): return self

        def __exit__(self, exception_type, exception_instance, exception_traceback):
            if self.__manipulator.state: self.__manipulator.close(exception_info = (
                exception_type, exception_instance, exception_traceback
            ))

        def __init__(self, *arguments, **keywords):
            super().__init__()
            self.__keywords = keywords
            self.__arguments = arguments
            self.__manipulator = _make_context_manipulator(asynchronous = False)

    class _Result(object):
        Class = _Class

    return _Result


_private = _private()
try: Class = _private.Class
finally: del _private


# noinspection PyArgumentList
def make(*args, **kwargs): return Class(*args, **kwargs)
