#!/usr/bin/env python3
# -*- coding: utf-8 -*-

assert "__main__" != __name__


def _private():
    import typing
    import asyncio
    import contextlib

    from .. import context_manipulator as _context_manipulator_module

    _make_context_manipulator = _context_manipulator_module.make

    try: _asyncio_class = asyncio.TaskGroup
    except AttributeError: _asyncio_class = None

    def _cancel_tasks(tasks: typing.Iterable[asyncio.Task]):
        tasks = list(tasks)

        def _iteration():
            _task = tasks.pop(0)
            try:
                assert isinstance(_task, asyncio.Task)
                if not _task.done(): _task.cancel()
            except BaseException:
                _iteration()
                raise

        while tasks: _iteration()

    def _make_coroutine(awaitable: typing.Awaitable):
        if asyncio.iscoroutine(awaitable): return awaitable

        async def _wrapper():
            if isinstance(awaitable, asyncio.Task):
                await asyncio.gather(awaitable, return_exceptions = True)
                if not awaitable.cancelled():
                    _exception = awaitable.exception()
                    if _exception is not None: raise _exception

            return await awaitable

        return _wrapper()

    if _asyncio_class is None:
        @contextlib.asynccontextmanager
        async def _open_context(lazy: bool):
            try: assert lazy is True
            except AssertionError: raise NotImplementedError()

            _tasks = list()

            async def _finalizer():
                if not _tasks: return

                await asyncio.wait(_tasks, return_when = asyncio.FIRST_EXCEPTION)
                _cancel_tasks(tasks = _tasks)
                await asyncio.gather(*_tasks, return_exceptions = True)

                def _generator():
                    for _task in _tasks:
                        assert _task.done()
                        if _task.cancelled(): continue
                        _exception = _task.exception()
                        if _exception is None: continue
                        yield _exception

                _exceptions = list(_generator())

                def _iteration():
                    if not _exceptions: return
                    _current = _exceptions.pop()
                    assert isinstance(_current, BaseException)
                    try: raise _current
                    finally: _iteration()

                _iteration()

            class _Context(object):
                @staticmethod
                def wait(return_when): return asyncio.wait(_tasks, return_when = return_when)

                @staticmethod
                def cancel(): _cancel_tasks(tasks = _tasks)

                @staticmethod
                def spawn(awaitable: typing.Awaitable):
                    _task = asyncio.create_task(_make_coroutine(awaitable = awaitable))
                    _tasks.append(_task)
                    return _task

            try: yield _Context()
            finally: await asyncio.shield(_finalizer())

    else:
        @contextlib.asynccontextmanager
        async def _open_context(lazy: bool):
            assert isinstance(lazy, bool)

            class _BaseContext(object):
                @staticmethod
                def wait(return_when): return asyncio.wait(_tasks, return_when = return_when)

                @staticmethod
                def cancel(): _cancel_tasks(tasks = _tasks)

            if lazy:
                _tasks = list()

                async def _finalizer():
                    if not _tasks: return
                    async with _asyncio_class() as _lazy_asyncio_instance:
                        for _task in _tasks: _lazy_asyncio_instance.create_task(_make_coroutine(awaitable = _task))

                class _Context(_BaseContext):
                    @staticmethod
                    def spawn(awaitable: typing.Awaitable):
                        _task = asyncio.create_task(_make_coroutine(awaitable = awaitable))
                        _tasks.append(_task)
                        return _task

                try: yield _Context()
                finally: await asyncio.shield(_finalizer())

                return

            async with _asyncio_class() as _asyncio_instance:
                class _Context(_BaseContext):
                    @staticmethod
                    def spawn(awaitable: typing.Awaitable): return _asyncio_instance.create_task(_make_coroutine(
                        awaitable = awaitable
                    ))

                yield _Context()

    class _Class(object):
        @property
        def lazy(self): return self.__lazy

        def wait(self, return_when = asyncio.ALL_COMPLETED):
            _context = self.__manipulator.context
            assert _context is not None
            return _context.wait(return_when = return_when)

        def spawn(self, awaitable: typing.Awaitable):
            _context = self.__manipulator.context
            assert _context is not None
            return _context.spawn(awaitable = awaitable)

        def cancel(self):
            _context = self.__manipulator.context
            assert _context is not None
            return _context.cancel()

        async def __aenter__(self):
            await self.__manipulator.open(_open_context(lazy = self.__lazy))
            return self

        async def __aexit__(self, exception_type, exception_instance, exception_traceback):
            await self.__manipulator.close(exception_info = (exception_type, exception_instance, exception_traceback))

        def __init__(self, lazy: bool = False):
            super().__init__()
            if _asyncio_class is None:
                try: assert lazy is True
                except AssertionError: raise NotImplementedError()
            else: assert isinstance(lazy, bool)
            _manipulator = _make_context_manipulator(asynchronous = True)
            self.__lazy = lazy
            self.__manipulator = _manipulator

    class _Result(object):
        Class = _Class

    return _Result


_private = _private()
try: Class = _private.Class
finally: del _private


# noinspection PyArgumentList
def make(*args, **kwargs): return Class(*args, **kwargs)
