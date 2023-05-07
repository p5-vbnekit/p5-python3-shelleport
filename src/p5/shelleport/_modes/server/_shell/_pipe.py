#!/usr/bin/env python3
# -*- coding: utf-8 -*-

assert "__main__" != __name__


def _private():
    import os
    import typing
    import asyncio
    import contextlib

    from .... import _common as _common_module

    _protocol = _common_module.protocol.make()
    _valid_modes = {"r", "w"}
    _make_thread_pool = _common_module.asynchronous_tools.thread_pool.make
    _make_asynchronizer = _common_module.asynchronous_tools.asynchronizer.make

    @contextlib.asynccontextmanager
    async def _open_asynchronizer():
        with _make_thread_pool(max_workers = 1) as _thread_pool:
            def _spawn(delegate: typing.Callable): return _thread_pool(delegate)
            _thread_pool.open()
            yield await _thread_pool(_make_asynchronizer, executor = _spawn)

    @contextlib.contextmanager
    def _open_streams():
        _reader, _writer = os.pipe()

        try:
            assert isinstance(_reader, int)
            assert isinstance(_writer, int)
            _reader = os.fdopen(_reader, mode = "rb", buffering = -1, closefd = True)
            _writer = os.fdopen(_writer, mode = "wb", buffering = -1, closefd = True)
            yield _reader, _writer

        finally:
            try:
                if isinstance(_writer, int): os.close(_writer)
                else: _writer.close()

            finally:
                if isinstance(_reader, int): os.close(_reader)
                else: _reader.close()

    @contextlib.asynccontextmanager
    async def _open_manipulator(mode: str):
        assert isinstance(mode, str)
        assert mode in _valid_modes

        _loop = asyncio.get_running_loop()
        assert isinstance(_loop, asyncio.AbstractEventLoop)

        class _Context(object):
            event = asyncio.Event()
            class Manipulator(object): pass

        async with (
            _open_asynchronizer() as _asynchronizer,
            _asynchronizer(await _asynchronizer(_open_streams)) as (_reader, _writer)
        ):
            if "r" == mode:
                _Context.local_descriptor = await _asynchronizer(_reader.fileno)
                _Context.remote_stream = _writer

                async def _read():
                    if _Context.event is None: return bytes()
                    try:
                        try: _data = await _asynchronizer(
                            os.read, _Context.local_descriptor, _protocol.ideal_chunk_size
                        )
                        except BlockingIOError: _data = None
                        if _data is None:
                            _Context.event.clear()
                            _loop.add_reader(_Context.local_descriptor, _Context.event.set)
                            try: await _Context.event.wait()
                            finally: _loop.remove_reader(_Context.local_descriptor)
                            if _Context.event is None: return bytes()
                            assert _Context.event.is_set()
                            _data = await _asynchronizer(os.read, _Context.local_descriptor, _protocol.ideal_chunk_size)
                        assert isinstance(_data, bytes)
                        if not _data: _Context.event = None
                        return _data
                    except BaseException:
                        _Context.event = None
                        raise

                _Context.Manipulator.read = _read

            else:
                _Context.local_descriptor = await _asynchronizer(_writer.fileno)
                _Context.remote_stream = _reader

                async def _write(data: bytes):
                    assert isinstance(data, bytes)
                    assert data
                    data = memoryview(data)
                    if _Context.event is None: return 0

                    _counter = 0

                    try:
                        while data:
                            try: _size = await _asynchronizer(os.write, _Context.local_descriptor, data)
                            except BrokenPipeError: break
                            except BlockingIOError: _size = None
                            if _size is None:
                                _Context.event.clear()
                                _loop.add_writer(_Context.local_descriptor, _Context.event.set)
                                try: await _Context.event.wait()
                                finally: _loop.remove_writer(_Context.local_descriptor)
                                if _Context.event is None: break
                                assert _Context.event.is_set()
                                try: _size = await _asynchronizer(os.write, _Context.local_descriptor, data)
                                except BrokenPipeError: break
                            assert isinstance(_size, int)
                            if 0 == _size: break
                            assert 0 < _size
                            assert len(data) >= _size
                            _counter += _size
                            data = data[_size:]

                    except BaseException:
                        _Context.event = None
                        raise

                    if data: _Context.event = None
                    return _counter

                _Context.Manipulator.write = _write

            _Context.remote_descriptor = await asyncio.to_thread(_Context.remote_stream.fileno)

            class _Remote(object):
                @property
                def descriptor(self): return _Context.remote_descriptor

                @staticmethod
                async def close(): await asyncio.to_thread(_Context.remote_stream.close)

            _Context.Manipulator.remote = _Remote()
            await _asynchronizer(os.set_blocking, _Context.local_descriptor, False)
            try: yield _Context.Manipulator
            finally:
                if _Context.event is not None:
                    if not _Context.event.is_set(): _Context.event.set()
                    _Context.event = None

    class _Class(object):
        @property
        def mode(self): return self.__mode

        @property
        def state(self): return self.__state

        @property
        def remote(self):
            _manipulator = self.__manipulator
            return None if _manipulator is None else _manipulator.remote

        async def open(self):
            assert self.__state is None
            self.__state = False
            assert self.__context is None
            _context = _open_manipulator(mode = self.__mode)
            _manipulator = await _context.__aenter__()
            self.__state = True
            self.__context = _context
            self.__manipulator = _manipulator

        async def close(self):
            assert self.__state is True
            self.__state = False
            _context = self.__context
            assert _context is not None
            self.__context = None
            await _context.__aexit__(None, None, None)

        async def read(self):
            _manipulator = self.__manipulator
            assert _manipulator is not None, "not opened"
            return await _manipulator.read()

        async def write(self, data: bytes):
            assert isinstance(data, bytes)
            assert data
            _manipulator = self.__manipulator
            assert _manipulator is not None, "not opened"
            return await _manipulator.write(data = data)

        async def __aenter__(self): return self

        async def __aexit__(self, exception_type, exception_instance, exception_traceback):
            if self.__state is not True: return
            self.__state = False
            _context = self.__context
            assert _context is not None
            self.__context = None
            await _context.__aexit__(exception_type, exception_instance, exception_traceback)

        def __init__(self, mode: str):
            super().__init__()
            assert isinstance(mode, str)
            assert mode in _valid_modes
            self.__mode = mode
            self.__state = None
            self.__context = None
            self.__manipulator = None

    class _Result(object):
        Class = _Class

    return _Result


_private = _private()
try: Class = _private.Class
finally: del _private


# noinspection PyArgumentList
def make(*args, **kwargs): return Class(*args, **kwargs)
