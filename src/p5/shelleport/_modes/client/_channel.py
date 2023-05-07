#!/usr/bin/env python3
# -*- coding: utf-8 -*-

assert "__main__" != __name__


def _private():
    import typing
    import contextlib

    from ... import _common as _common_module

    _open_stream_reader = _common_module.asynchronous_tools.non_blocking_io.open_reader
    _open_stream_writer = _common_module.asynchronous_tools.non_blocking_io.open_writer

    _mode_mapping = {
        "r": lambda stream: _open_stream_reader(source = stream),
        "w": lambda stream: _open_stream_writer(destination = stream),
    }
    _stream_type_hint = typing.Union[int, typing.IO]

    def _make_delegate(stream: _stream_type_hint, mode: str = None):
        if isinstance(stream, int): assert isinstance(mode, str)
        elif mode is None:
            if stream.readable(): mode = "r"
            else:
                mode = "w"
                assert stream.writable()
        else: assert isinstance(mode, str)
        return lambda: _mode_mapping[mode](stream = stream)

    @contextlib.asynccontextmanager
    async def _open_stream(
        stream: _stream_type_hint, mode: typing.Optional[str], cleaner: typing.Optional[typing.Callable]
    ):
        _delegate = _make_delegate(stream = stream, mode = mode)

        try:
            async with _delegate() as stream: yield stream
        finally:
            if cleaner is not None: await cleaner()

    class _Class(object):
        @property
        def mode(self): return self.__mode

        @property
        def state(self):
            if self.__context is None: return None
            return self.__operation is not None

        @property
        def stream(self): return self.__stream

        async def open(self):
            assert self.__context is None, "opened already"
            _context = _open_stream(stream = self.__stream, mode = self.__mode, cleaner = self.__cleaner)
            _operation = await _context.__aenter__()
            self.__context = _context
            self.__operation = _operation

        async def close(self):
            _context = self.__context
            assert _context is not None, "not opened"
            self.__context = None
            self.__operation = None
            await _context.__aexit__(None, None, None)

        async def __call__(self, *args, **kwargs):
            assert self.__context is not None, "not opened"
            assert self.__operation is not None, "broken"
            try: _result = await self.__operation(*args, **kwargs)
            except BaseException:
                self.__operation = None
                raise
            return _result

        async def __aenter__(self): return self

        async def __aexit__(self, exception_type, exception_instance, exception_traceback):
            _context = self.__context
            self.__context = None
            self.__operation = None
            if _context is not None: await _context.__aexit__(exception_type, exception_instance, exception_traceback)

        def __init__(self, stream: _stream_type_hint, mode: str = None, cleaner: typing.Callable = None):
            super().__init__()
            if isinstance(stream, int):
                assert isinstance(mode, str)
                assert mode in _mode_mapping
            elif mode is not None:
                assert isinstance(mode, str)
                assert mode in _mode_mapping
            self.__mode = mode
            self.__stream = stream
            self.__context = None
            self.__cleaner = cleaner
            self.__operation = None

    class _Result(object):
        Class = _Class

    return _Result


_private = _private()
try: Class = _private.Class
finally: del _private


# noinspection PyArgumentList
def make(*args, **kwargs): return Class(*args, **kwargs)
