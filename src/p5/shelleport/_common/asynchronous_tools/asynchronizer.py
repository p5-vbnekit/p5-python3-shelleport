#!/usr/bin/env python3
# -*- coding: utf-8 -*-

assert "__main__" != __name__


def _private():
    import types
    import typing
    import threading

    _executor_type_hint = typing.Callable[[typing.Callable], typing.Awaitable]

    def _close_generator(generator: typing.Generator):
        assert isinstance(generator, types.GeneratorType)
        generator.close()
        for _item in generator:
            try: raise OverflowError("unexpected iteration occurred")
            except OverflowError: raise ValueError("unexpected generator state")

    class _Wrapper(object):
        @property
        def target(self): return self.__target

        @property
        def executor(self): return self.__executor

        def __await__(self): return self.__executor(self.__target).__await__()

        async def __aiter__(self):
            class _Stop(Exception): pass

            class _Shared(object):
                flag = False
                lock = threading.Condition()

            def _generator(): yield from self.__target

            _generator = await self.__executor(_generator)

            try:
                _iterator = await self.__executor(lambda: iter(_generator))

                def _next():
                    with _Shared.lock:
                        if _Shared.flag is None: raise _Stop()
                        assert _Shared.flag is False
                        _Shared.flag = True

                    try: return next(_iterator)
                    except StopIteration: raise _Stop()

                    finally:
                        with _Shared.lock:
                            assert _Shared.flag is True
                            _Shared.flag = False
                            _Shared.lock.notify_all()

                while True:
                    try: _value = await self.__executor(_next)
                    except _Stop: break
                    yield _value

            finally:
                with _Shared.lock:
                    while _Shared.flag: _Shared.lock.wait()
                    assert _Shared.flag is False
                    _Shared.flag = None

                await self.__executor(lambda: _close_generator(generator = _generator))

        async def __aenter__(self): return await self.__executor(self.__target.__enter__)

        async def __aexit__(self, exception_type, exception_instance, exception_traceback):
            return await self.__executor(lambda: self.__target.__exit__(
                exception_type, exception_instance, exception_traceback
            ))

        def __init__(self, target, executor: _executor_type_hint):
            super().__init__()
            self.__target = target
            self.__executor = executor

    def _make_target(arguments: typing.Iterable, keywords: typing.Dict[str, typing.Any]):
        keywords = dict(**keywords)
        arguments = list(arguments)
        _target = arguments.pop(0)
        if not (arguments or keywords): return _target
        return lambda: _target(*arguments, **keywords)

    class _Class(object):
        @property
        def executor(self): return self.__executor

        def __call__(self, *arguments, **keywords): return _Wrapper(
            target = _make_target(arguments, keywords), executor = self.__executor
        )

        def __init__(self, executor: _executor_type_hint):
            super().__init__()
            self.__executor = executor

    class _Result(object):
        Class = _Class

    return _Result


_private = _private()
try: Class = _private.Class
finally: del _private


# noinspection PyArgumentList
def make(*args, **kwargs): return Class(*args, **kwargs)
