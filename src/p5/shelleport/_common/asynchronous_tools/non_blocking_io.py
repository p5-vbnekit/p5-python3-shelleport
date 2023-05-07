#!/usr/bin/env python3
# -*- coding: utf-8 -*-

assert "__main__" != __name__


def _private():
    import os
    import typing
    import asyncio
    import contextlib

    from . import thread_pool as _thread_pool_module
    from . import asynchronizer as _asynchronizer_module

    from .. import protocol as _protocol_module

    _protocol = _protocol_module.make()
    _make_thread_pool = _thread_pool_module.make
    _make_asynchronizer = _asynchronizer_module.make

    @contextlib.asynccontextmanager
    async def _open_asynchronizer():
        with _make_thread_pool(max_workers = 1) as _thread_pool:
            def _spawn(delegate: typing.Callable): return _thread_pool(delegate)
            _thread_pool.open()
            yield await _thread_pool(_make_asynchronizer, executor = _spawn)

    @contextlib.contextmanager
    def _open_stream(stream: typing.Union[int, typing.IO[bytes]], mode: str):
        if isinstance(stream, int): _descriptor = stream
        else:
            _descriptor = stream.fileno()
            assert isinstance(_descriptor, int)

        assert 0 <= _descriptor
        stream = _descriptor

        assert isinstance(mode, str)
        assert mode in {"r", "w"}

        _descriptor = os.dup(stream)
        assert isinstance(_descriptor, int)
        assert (0 <= _descriptor) and (stream != _descriptor)

        try:
            with os.fdopen(_descriptor, mode = f"{mode}b", buffering = 0, closefd = False) as _stream:
                assert _descriptor == _stream.fileno()
                _blocking = os.get_blocking(_descriptor)
                try:
                    assert isinstance(_blocking, bool)
                    if _blocking: os.set_blocking(_descriptor, False)
                    yield _descriptor
                finally:
                    if _blocking: os.set_blocking(_descriptor, True)
        finally: os.close(_descriptor)

    @contextlib.asynccontextmanager
    async def _open_reader(source: typing.Union[int, typing.IO[bytes]]):
        _loop = asyncio.get_running_loop()
        assert isinstance(_loop, asyncio.AbstractEventLoop)

        class _Context(object):
            event = asyncio.Event()

        async def _coroutine(delegate: typing.Callable, size: typing.Optional[int]):
            if size is None: size = _protocol.ideal_chunk_size
            else:
                assert isinstance(size, int)
                if -1 == size: size = _protocol.ideal_chunk_size
                else: assert 0 < size

            if _Context.event is None: return bytes()

            try:
                _chunk = await delegate(size = size)
                assert isinstance(_chunk, bytes)

            except BaseException:
                _Context.event = None
                raise

            if not _chunk: _Context.event = None
            return _chunk

        async with _open_asynchronizer() as _asynchronizer:
            async def _operation(size: int): return await _asynchronizer(os.read, source, size)
            async with _asynchronizer(await _asynchronizer(_open_stream, stream = source, mode = "r")) as source:
                async def _delegate(size: int):
                    try: return await _operation(size = size)
                    except BlockingIOError:
                        _Context.event.clear()
                        _loop.add_reader(source, _Context.event.set)
                        try:
                            await _Context.event.wait()
                            assert _Context.event.is_set()
                        finally: _loop.remove_reader(source)
                        return await _operation(size = size)
                _result: typing.Callable[
                    [typing.Optional[int]], typing.Awaitable[bytes]
                ] = lambda size = None: _coroutine(delegate = _delegate, size = size)
                yield _result

    @contextlib.asynccontextmanager
    async def _open_writer(destination: typing.Union[int, typing.IO[bytes]]):
        _loop = asyncio.get_running_loop()
        assert isinstance(_loop, asyncio.AbstractEventLoop)

        class _Context(object):
            event = asyncio.Event()

        async def _coroutine(delegate: typing.Callable, data: bytes):
            assert isinstance(data, bytes)
            assert data
            if _Context.event is None: return 0
            data = memoryview(data)
            _counter = 0
            try:
                while data:
                    _size = await delegate(data = data)
                    assert isinstance(_size, int)
                    assert 0 < _size
                    assert len(data) >= _size
                    _counter = _counter + _size
                    data = data[_size:]
            except BaseException:
                _Context.event = None
                raise
            return _counter

        async with _open_asynchronizer() as _asynchronizer:
            async def _operation(data: bytes): return await _asynchronizer(os.write, destination, data)
            async with _asynchronizer(await _asynchronizer(
                _open_stream, stream = destination, mode = "w"
            )) as destination:
                async def _delegate(data):
                    try: return await _operation(data = data)
                    except BlockingIOError:
                        _Context.event.clear()
                        _loop.add_writer(destination, _Context.event.set)
                        try:
                            await _Context.event.wait()
                            assert _Context.event.is_set()
                        finally: _loop.remove_writer(destination)
                        return await _operation(data = data)
                _result: typing.Callable[[bytes], typing.Awaitable[int]] = lambda data: _coroutine(
                    delegate = _delegate, data = data
                )
                yield _result

    class _Result(object):
        open_reader = _open_reader
        open_writer = _open_writer

    return _Result


_private = _private()
try:
    open_reader = _private.open_reader
    open_writer = _private.open_writer
finally: del _private
