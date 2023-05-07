#!/usr/bin/env python3
# -*- coding: utf-8 -*-

assert "__main__" != __name__


def _private():
    import os
    import sys
    import typing
    import asyncio
    import contextlib

    from . import _channel as _channel_module
    from ... import _common as _common_module

    _Channel = _channel_module.Class
    _IterationController = _common_module.asynchronous_tools.IterationController

    _protocol = _common_module.protocol.make()
    _make_channel = _channel_module.make
    _make_task_group = _common_module.asynchronous_tools.task_group.make
    _make_thread_pool = _common_module.asynchronous_tools.thread_pool.make

    def _make_start_request(export: typing.Iterable[str], arguments: typing.Iterable[str]):
        _environment = dict()
        for _value in export:
            assert isinstance(_value, str)
            assert _value.lstrip() == _value
            try: _key, _value = _value.split("=")
            except ValueError:
                _key = _value
                _value = None
            else: assert isinstance(_value, str)
            assert _key
            _key, = f"{_key}\r\n".splitlines()
            assert _key not in _environment
            if _value is None: _value = os.environ[_key]
            _environment[_key] = _value
        for _argument in arguments: assert isinstance(_argument, str)
        return {"environment": _environment, "arguments": arguments}

    @contextlib.asynccontextmanager
    async def _open_peer(peer: dict):
        assert isinstance(peer, dict)
        _action = {
            "unix": lambda: asyncio.open_unix_connection(path = peer["path"]),
            "tcp": lambda: asyncio.open_connection(host = peer["host"], port = peer["port"])
        }[peer["type"]]
        try: _reader, _writer = await _action()
        except Exception: raise ConnectionError(peer)
        try: yield _reader, _writer
        finally: _writer.close()

    def _do_all(actions: typing.Iterable[typing.Callable]):
        actions = list(actions).copy()

        def _iteration():
            if not actions: return False
            _action = actions.pop(0)
            try: _action()
            except BaseException:
                _iteration()
                raise
            return True

        while _iteration(): pass

    @contextlib.asynccontextmanager
    async def _open_stdio():
        _descriptors = set()

        def _catch_streams(streams: typing.Iterable[typing.IO]):
            _descriptor = None
            for _stream in streams:
                _number = _stream.fileno()
                assert isinstance(_number, int)
                if _descriptor is None:
                    assert 0 <= _number
                    _descriptor = _number
                else: assert _descriptor == _number
                yield _stream.close
            if _descriptor is None: return
            _descriptors.add(_descriptor)
            yield lambda: os.close(_descriptor)

        @contextlib.asynccontextmanager
        async def _open_channel(*args, **kwargs):
            async with _make_channel(*args, **kwargs) as _channel:
                await _channel.open()
                yield _channel

        with _make_thread_pool(max_workers = 1) as _thread_pool:
            async def _make_cleaner(streams: typing.Iterable[typing.IO]):
                _actions = await _thread_pool(lambda: tuple(_catch_streams(streams = streams)))
                return lambda: _thread_pool(_do_all, actions = _actions)

            _thread_pool.open()

            _stdin_cleaner = await _make_cleaner(streams = (sys.stdin.buffer, sys.stdin))
            _stdout_cleaner = await _make_cleaner(streams = (sys.stdout.buffer, sys.stdout))

            async with (
                _open_channel(stream = sys.stdin, mode = "r", cleaner = _stdin_cleaner) as _stdin,
                _open_channel(stream = sys.stdout, mode = "w", cleaner = _stdout_cleaner) as _stdout,
                _open_channel(stream = sys.stderr, mode = "w") as _stderr
            ): yield _stdin, _stdout, _stderr

    def _check_accepted_response(value: dict):
        assert isinstance(value, dict)
        assert value
        value = value.copy()
        assert value.pop("accepted") is True
        assert not value

    async def _read_coroutine(
        reader: typing.AsyncIterable[dict], writer: typing.Callable,
        stdin: _Channel, stdout: _Channel, stderr: _Channel
    ):
        _result = None
        _closed = set()
        _message = None
        _exception = None
        assert isinstance(stdin, _Channel)
        assert isinstance(stdout, _Channel)
        assert isinstance(stderr, _Channel)
        _channels = {"stdin": stdin, "stdout": stdout, "stderr": stderr}

        async def _generator():
            while True:
                try: _item = await _IterationController.anext(reader)
                except StopAsyncIteration: break
                except (BrokenPipeError, ConnectionResetError):
                    if _result is None: raise
                    break
                yield _item
            async for _item in reader: raise OverflowError(f"unexpected message: {_item}")

        async for _message in _generator():
            assert isinstance(_message, dict)
            assert _message, "empty message"
            try: _exception = _message.pop("exception")
            except KeyError: pass
            else:
                assert not _message
                assert isinstance(_exception, str)
                assert _exception
                continue
            try: _result = _message.pop("result")
            except KeyError: pass
            else:
                assert not _message
                assert isinstance(_result, int)
                continue
            assert _exception is None, "unexpected message after exception"
            assert _result is None, "unexpected message after result"
            _channel_key = _message.pop("channel")
            assert isinstance(_channel_key, str)
            _channel = _channels[_channel_key]
            try: _blob = _message.pop("blob")
            except KeyError: _blob = None
            assert not _message
            if _blob is None:
                assert _channel_key not in _closed
                _closed.add(_channel_key)
                if _channel.state: await _channel.close()
                continue
            assert isinstance(_blob, bytes)
            assert _blob
            assert _channel is not stdin
            if not _channel.state: continue
            try: await _channel(data = _blob)
            except BrokenPipeError: await writer({"channel": _channel_key})

        if _exception is not None:
            assert isinstance(_exception, str)
            assert _exception
            print(_exception, file = sys.stderr, flush = True)
            try: raise RuntimeError("remote exception received")
            finally: assert _result is None

        assert isinstance(_result, int), "unexpected peer disconnect"
        return _result

    async def _write_coroutine(peer: typing.Callable, stdin: _Channel):
        assert isinstance(stdin, _Channel)

        _peer_state = True

        try:
            while True:
                _blob = await stdin()
                assert isinstance(_blob, bytes)
                if not _blob: break
                _peer_state = False
                try: await peer({"channel": "stdin", "blob": _blob})
                except (BrokenPipeError, ConnectionResetError): break
                _peer_state = True

        finally:
            try:
                if _peer_state:
                    try: await peer({"channel": "stdin"})
                    except (BrokenPipeError, ConnectionResetError): pass

            finally: await stdin.close()

    async def _coroutine(peer: dict, export: typing.Iterable[str], arguments: typing.Iterable[str]):
        _start_request = await asyncio.to_thread(lambda: _make_start_request(export = export, arguments = arguments))
        async with _open_peer(peer = peer) as (_peer_reader, _peer_writer):
            async def _protocol_reader_source(size: int):
                assert isinstance(size, int)
                assert 0 < size
                return await _peer_reader.read(size)

            async def _protocol_writer_destination(data: typing.Iterable[bytes]):
                for data in data:
                    assert isinstance(data, bytes)
                    assert data
                    _peer_writer.write(data)
                await _peer_writer.drain()

            async with (
                _open_stdio() as (_stdin, _stdout, _stderr),
                _protocol.open_reader(source = _protocol_reader_source) as _protocol_reader,
                _protocol.open_writer(destination = _protocol_writer_destination) as _protocol_writer
            ):
                await _protocol_writer(message = _start_request)
                _response = await _IterationController.anext(target = _protocol_reader)
                await asyncio.to_thread(lambda: _check_accepted_response(value = _response))

                _result = None

                async with _make_task_group(lazy = True) as _task_group:
                    try:
                        _read_task = _task_group.spawn(awaitable = _read_coroutine(
                            reader = _protocol_reader, writer = _protocol_writer,
                            stdin = _stdin, stdout = _stdout, stderr = _stderr
                        ))
                        _write_task = _task_group.spawn(awaitable = _write_coroutine(
                            peer = _protocol_writer, stdin = _stdin
                        ))
                        await _task_group.wait(return_when = asyncio.FIRST_COMPLETED)
                        if _write_task.done(): await _write_task
                        _result = await _read_task
                    finally: _task_group.cancel()

                assert isinstance(_result, int)
                return _result

    def _routine(*args, **kwargs):
        _result = asyncio.run(_coroutine(*args, **kwargs))
        assert isinstance(_result, int)
        exit(_result)

    class _Result(object):
        routine = _routine

    return _Result


_private = _private()
try: routine = _private.routine
finally: del _private
