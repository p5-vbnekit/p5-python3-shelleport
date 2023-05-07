#!/usr/bin/env python3
# -*- coding: utf-8 -*-

assert "__main__" != __name__


def _private():
    import time
    import json
    import typing
    import asyncio
    import contextlib

    from . import asynchronous_tools as _asynchronous_tools_module

    _Asynchronizer = _asynchronous_tools_module.Asynchronizer

    _make_thread_pool = _asynchronous_tools_module.thread_pool.make
    _make_asynchronizer = _asynchronous_tools_module.asynchronizer.make
    _make_iteration_controller = _asynchronous_tools_module.iteration_controller.make

    _magic_text = f"{__name__}:magic"
    _zero_byte = b"\x00"
    _chunk_timeout = +9.0e+0

    if isinstance(asyncio.TimeoutError, TimeoutError): _KeepAliveTimeoutError = TimeoutError
    else: _KeepAliveTimeoutError = asyncio.TimeoutError

    class _ReadBuffer(object):
        @property
        def size(self): return self.__size

        def push(self, chunk: bytes):
            assert isinstance(chunk, bytes)
            assert chunk
            self.__current.append(chunk)
            self.__size += len(chunk)

        def pop_blob(self, size: int):
            assert isinstance(size, int)
            assert 0 < size
            assert self.__size >= size
            assert self.__current
            assert not self.__prefix
            _size = size
            _chunks = []
            _temporary = self.__current.copy()
            while 0 < _size:
                _chunks.append(_temporary.pop(0))
                _size -= len(_chunks[-1])
            assert _chunks
            if 0 > _size:
                _temporary.insert(0, _chunks[-1][_size:])
                _chunks[-1] = _chunks[-1][:_size]
            else: assert 0 == _size
            if 1 < len(_chunks): _chunks = bytes().join(_chunks)
            else: _chunks, = _chunks
            assert _chunks
            assert size == len(_chunks)
            self.__current = _temporary
            self.__size -= size
            return _chunks

        def pop_message(self):
            assert self.__current
            assert 0 < self.__size
            _zero = None
            _chunks = []
            _temporary = self.__current.copy()
            while _temporary:
                _chunks.append(_temporary.pop(0))
                try: _zero = _chunks[-1].index(_zero_byte)
                except ValueError: continue
                break
            assert _chunks
            if _zero is None:
                assert not _temporary
                self.__prefix.extend(self.__current)
                self.__current.clear()
                return None
            _chunks = tuple(self.__pop_message_generator(chunks = _chunks, zero = _zero))
            if _chunks[-1]: _temporary.insert(0, _chunks[-1])
            _chunks = bytes().join(_chunks[:-1])
            _size = 1 + len(_chunks)
            assert 1 < _size
            assert self.__size >= _size
            self.__prefix.clear()
            self.__current = _temporary
            self.__size -= _size
            return _chunks

        def __init__(self):
            super().__init__()
            self.__size = 0
            self.__prefix = []
            self.__current = []

        def __pop_message_generator(self, chunks: typing.Iterable[bytes], zero: int):
            assert isinstance(zero, int)
            assert 0 <= zero
            yield from self.__prefix
            _previous = None
            for _current in chunks:
                assert isinstance(_current, bytes)
                assert _current
                if _previous is not None: yield _previous
                _previous = _current
            assert _previous is not None
            if 0 < zero:
                _current = _previous[:zero]
                assert zero == len(_current)
                yield _current
            yield _previous[1 + zero:]

    def _serialize_message(message: dict):
        assert isinstance(message, dict)
        message = message.copy()

        try: _magic = message["magic"]
        except KeyError: message["magic"] = _magic_text
        else:
            assert isinstance(_magic, str)
            assert _magic_text == _magic
        message = message.copy()
        try: _blob = message["blob"]
        except KeyError: _blob = None
        else:
            assert isinstance(_blob, bytes)
            assert _blob
            message["blob"] = len(_blob)

        message = json.dumps(message).encode("utf-8")
        if _blob is None: return message,
        return message, _blob

    _keep_alive_message, = _serialize_message(message = {})
    _keep_alive_message = bytes().join((_keep_alive_message, _zero_byte))

    _ideal_chunk_size = 8 * 1024 * 1024
    _max_message_size = _ideal_chunk_size
    _min_message_size = len(_keep_alive_message)
    assert _min_message_size <= _max_message_size

    @contextlib.asynccontextmanager
    async def _open_asynchronizer():
        with _make_thread_pool(max_workers = 1) as _thread_pool:
            def _spawn(delegate: typing.Callable): return _thread_pool(delegate)
            _thread_pool.open()
            yield await _thread_pool(_make_asynchronizer, executor = _spawn)

    def _parse_message(value: bytes):
        assert isinstance(value, bytes)
        _size = 1 + len(value)
        assert _min_message_size <= _size
        assert _max_message_size >= _size
        value = value.decode("utf-8")
        value, = value.splitlines()
        assert value.strip() == value
        value = json.loads(value)
        assert isinstance(value, dict)
        _magic = value.pop("magic")
        assert isinstance(_magic, str)
        assert _magic == _magic_text
        return value

    def _pop_messages(buffer: _ReadBuffer):
        assert isinstance(buffer, _ReadBuffer)
        while 0 < buffer.size:
            _message = buffer.pop_message()
            if _message is None: return
            _message = _parse_message(value = _message)
            assert isinstance(_message, dict)
            if _message: yield _message

    async def _read_blob(size: int, buffer: _ReadBuffer, reader: typing.Callable, asynchronizer: _Asynchronizer):
        assert isinstance(size, int)
        assert 0 < size
        assert isinstance(buffer, _ReadBuffer)
        assert isinstance(asynchronizer, _Asynchronizer)
        size = size + 1
        _chunks = []
        _buffer_size = buffer.size
        if 0 < _buffer_size:
            _size = min(size, _buffer_size)
            await asynchronizer(lambda: _chunks.append(buffer.pop_blob(size = _size)))
            assert isinstance(_chunks[0], bytes)
            assert _chunks[0]
            assert _size == len(_chunks[0])
            size -= _size
            _buffer_size -= _size
            assert _buffer_size == buffer.size
            if 0 < _buffer_size: assert 0 == size
            else:
                assert 0 <= size
                assert 0 == _buffer_size
        while 0 < size:
            _chunk = await reader(size = size)
            assert isinstance(_chunk, bytes)
            assert _chunk
            _chunk_size = len(_chunk)
            assert 0 < _chunk_size
            assert size >= _chunk_size
            size -= _chunk_size
            _chunks.append(_chunk)
        assert _chunks
        assert 0 == _chunks[-1][-1]
        if 1 == len(_chunks): return _chunks[0][:-1]
        _chunks[-1] = _chunks[-1][:-1]
        return bytes().join(_chunks)

    def _generate_serialized_chunks(message: dict):
        for message in _serialize_message(message = message):
            yield message
            yield _zero_byte

    @contextlib.asynccontextmanager
    async def _open_reader(source: typing.Callable):
        async with _open_asynchronizer() as _asynchronizer:
            _buffer = await _asynchronizer(_ReadBuffer)

            async def _generator():
                while True:
                    _size = await _asynchronizer(lambda: _max_message_size - _buffer.size)
                    assert await _asynchronizer(lambda: 0 < _size)
                    _chunk = await asyncio.wait_for(source(size = _size), timeout = _chunk_timeout)
                    assert await _asynchronizer(isinstance, _chunk, bytes)
                    if not _chunk: break
                    await _asynchronizer(_buffer.push, chunk = _chunk)
                    async for _message in _asynchronizer(_pop_messages(buffer = _buffer)):
                        try: _blob = await _asynchronizer(_message.pop, "blob")
                        except KeyError: pass
                        else:
                            assert await _asynchronizer(lambda: isinstance(_blob, int) and (0 < _blob))
                            _message["blob"] = await _read_blob(
                                size = _blob, buffer = _buffer, reader = source, asynchronizer = _asynchronizer
                            )
                            assert await _asynchronizer(lambda: _blob == len(_message["blob"]))
                        yield _message
                    assert await _asynchronizer(lambda: _max_message_size > _buffer.size)

            async with await _asynchronizer(_make_iteration_controller, factory = _generator) as _generator:
                await _generator.open()
                yield _generator.make_iterator()

            assert await _asynchronizer(lambda: 0 == _buffer.size)

    @contextlib.asynccontextmanager
    async def _open_writer(destination: typing.Callable[[typing.Iterable[bytes]], typing.Awaitable[None]]):
        _barrier_step = _chunk_timeout / +3.0e+0

        class _Context(object):
            lock = asyncio.Condition()
            barrier = None

        async def _keep_alive_coroutine():
            async with _Context.lock:
                while True:
                    if _Context.barrier is False: return
                    if _Context.barrier is None:
                        await _Context.lock.wait()
                        continue
                    assert isinstance(_Context.barrier, float)
                    _time = time.monotonic()
                    if _Context.barrier > _time:
                        try: await asyncio.wait_for(_Context.lock.wait(), timeout = _Context.barrier - _time)
                        except _KeepAliveTimeoutError: pass
                        continue
                    await destination((_keep_alive_message, ))
                    _Context.barrier = _barrier_step + time.monotonic()

        _keep_alive_task = asyncio.create_task(_keep_alive_coroutine())

        async with _open_asynchronizer() as _asynchronizer:
            async def _coroutine(message: dict):
                assert isinstance(message, dict)
                assert message
                message = await _asynchronizer(lambda: tuple(_generate_serialized_chunks(message = message)))
                async with _Context.lock:
                    assert (_Context.barrier is None) or isinstance(_Context.barrier, float), "writer closed"
                    try:
                        assert not _keep_alive_task.done()
                        await destination(message)
                        _Context.barrier = _barrier_step + time.monotonic()
                    except BaseException:
                        _Context.barrier = False
                        raise
                    finally: _Context.lock.notify_all()

            try: yield _coroutine
            finally:
                async with _Context.lock:
                    _Context.barrier = False
                    _Context.lock.notify_all()
                await asyncio.gather(_keep_alive_task, return_exceptions = True)

    class _Class(object):
        @property
        def zero_byte(self): return _zero_byte

        @property
        def ideal_chunk_size(self): return _ideal_chunk_size

        @staticmethod
        def serialize_message(message: dict): return _serialize_message(message = message)

        @staticmethod
        def open_reader(source: typing.Callable): return _open_reader(source = source)

        @staticmethod
        def open_writer(destination: typing.Callable): return _open_writer(destination = destination)

    class _Result(object):
        Class = _Class

    return _Result


_private = _private()
try: Class = _private.Class
finally: del _private


# noinspection PyArgumentList
def make(*args, **kwargs): return Class(*args, **kwargs)
