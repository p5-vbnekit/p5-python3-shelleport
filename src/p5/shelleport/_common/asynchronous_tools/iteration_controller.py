#!/usr/bin/env python3
# -*- coding: utf-8 -*-

assert "__main__" != __name__


def _private():
    import types
    import typing
    import asyncio
    import contextlib

    from .. import context_manipulator as _context_manipulator_module

    _iterable_type_hint = typing.AsyncIterable
    _iteration_type_hint = typing.Callable[[], typing.Awaitable]
    _factory_type_hint = typing.Callable[[], typing.AsyncIterable]
    _make_context_manipulator = _context_manipulator_module.make

    if "aiter" in dir(__builtins__):
        def _aiter(target): return aiter(target)
        def _anext(target): return anext(target)
    else:
        def _aiter(target): return target.__aiter__()
        def _anext(target): return target.__anext__()

    @typing.overload
    def _make_factory(iterable: _iterable_type_hint) -> _factory_type_hint: ...

    @typing.overload
    def _make_factory(iteration: _iteration_type_hint) -> _factory_type_hint: ...

    def _make_factory(
        iterable: _iterable_type_hint = None,
        iteration: _iteration_type_hint = None
    ):
        if iteration is None:
            assert iterable is not None
            return lambda: iterable

        async def _generator():
            while True:
                _value = await iteration()
                try: _value = await _value
                except StopAsyncIteration: break
                yield _value

        return lambda: _generator()

    async def _close_source(source: typing.AsyncIterable):
        if not isinstance(source, types.AsyncGeneratorType): return

        await source.aclose()

        async for _item in source:
            try: raise OverflowError("unexpected iteration occurred")
            except OverflowError: raise ValueError("unexpected generator state")

        try:
            assert source.ag_frame is None
            assert source.ag_await is None
            assert source.ag_running is False

        except AssertionError: raise ValueError("unexpected generator state")

    @contextlib.asynccontextmanager
    async def _open_context(factory: _factory_type_hint):
        _source = factory()

        try:
            _shutdown_event = asyncio.Event()
            _shutdown_event_task = asyncio.create_task(_shutdown_event.wait())

            try:
                async def _generator():
                    try:
                        while True:
                            _iteration_task = asyncio.create_task(_anext(target = _source))

                            try:
                                await asyncio.wait((
                                    _iteration_task, _shutdown_event_task
                                ), return_when = asyncio.FIRST_COMPLETED)
                                if _shutdown_event.is_set(): break
                                assert not _shutdown_event_task.done()
                                try: _value = await _iteration_task
                                except StopAsyncIteration: break
                                if _shutdown_event.is_set(): break
                                assert not _shutdown_event_task.done()
                                yield _value

                            finally:
                                if not _iteration_task.done(): _iteration_task.cancel()
                                await asyncio.gather(_iteration_task, return_exceptions = True)

                    finally: await _close_source(source = _source)

                _generator = _generator()
                try: yield _generator
                finally: await _generator.aclose()

            finally:
                try: _shutdown_event.set()
                finally: await asyncio.wait_for(_shutdown_event_task, timeout = +1.0e+0)

        finally: await _close_source(source = _source)

    class _Class(object):
        @property
        def factory(self): return self.__factory

        @property
        def generator(self): return self.__manipulator.context

        @staticmethod
        def anext(target): return _anext(target = target)

        @staticmethod
        def aiter(target): return _aiter(target = target)

        async def open(self): await self.__manipulator.open(manager = _open_context(factory = self.__factory))

        async def close(self): await self.__manipulator.close(exception_info = None)

        def make_iterator(self): return _aiter(target = self.generator)

        async def __aenter__(self): return self

        async def __aexit__(self, exception_type, exception_instance, exception_traceback):
            if self.__manipulator.state: await self.__manipulator.close(exception_info = (
                exception_type, exception_instance, exception_traceback
            ))

        @typing.overload
        def __init__(self, factory: _factory_type_hint): ...

        @typing.overload
        def __init__(self, iterable: _iterable_type_hint): ...

        @typing.overload
        def __init__(self, iteration: _iteration_type_hint): ...

        def __init__(self, *arguments, **keywords):
            super().__init__()

            if arguments:
                assert not keywords
                _factory, = arguments

            else:
                (_key, _value), = keywords.items()
                assert isinstance(_key, str)
                if "factory" == _key: _factory = _value
                else: _factory = _make_factory(**{_key: _value})

            _manipulator = _make_context_manipulator(asynchronous = True)

            self.__factory = _factory
            self.__manipulator = _manipulator

    class _Result(object):
        Class = _Class

    return _Result


_private = _private()
try: Class = _private.Class
finally: del _private


# noinspection PyArgumentList
def make(*args, **kwargs): return Class(*args, **kwargs)
