#!/usr/bin/env python3
# -*- coding: utf-8 -*-

assert "__main__" != __name__


def _private():
    import os
    import sys
    import typing
    import asyncio
    import traceback

    from . import _shell as _shell_module
    from . import _session as _session_module
    from . import _listener as _listener_module

    from ... import _common as _common_module

    _Shell = _shell_module.Class
    _SessionPeer = _session_module.Peer

    _make_session = _session_module.make
    _listener_coroutine = _listener_module.coroutine
    _open_stream_reader = _common_module.asynchronous_tools.non_blocking_io.open_reader
    _open_stream_writer = _common_module.asynchronous_tools.non_blocking_io.open_writer

    def _close_stdio():
        _descriptor = sys.stdin.fileno()
        sys.stdin.close()
        sys.stdin.buffer.close()
        os.close(_descriptor)
        _descriptor = sys.stdout.fileno()
        sys.stdout.close()
        sys.stdout.buffer.close()
        os.close(_descriptor)

    async def _make_shell(command: typing.Optional[typing.Iterable[str]]):
        if command is None: command = await _Shell.get_default_command()
        return await asyncio.to_thread(lambda: _Shell(command = command))

    def _regenerate_chunks(value: typing.Iterable[bytes]):
        for value in value:
            assert isinstance(value, bytes)
            assert value
            yield value

    def _make_listener_delegate(shell: _Shell):
        assert isinstance(shell, _Shell)

        async def _session_coroutine(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
            try:
                assert isinstance(writer, asyncio.StreamWriter)

                try:
                    assert isinstance(reader, asyncio.StreamReader)

                    class _Context(object):
                        reader = True
                        writer = True

                    def _make_read_delegate(size: int = None):
                        if size is None: return reader.read
                        assert isinstance(size, int)
                        assert 0 < size
                        return lambda: reader.read(size)

                    class _Peer(_SessionPeer):
                        @staticmethod
                        async def read(size: int = None):
                            _delegate = _make_read_delegate(size = size)

                            assert _Context.reader is True
                            _Context.reader = False

                            try:
                                _data = await _delegate()
                                assert isinstance(_data, bytes)

                            except BaseException:
                                _Context.reader = None
                                raise

                            else: _Context.reader = True if _data else None

                            return _data

                        @staticmethod
                        async def write(data: typing.Iterable[bytes]):
                            data = await asyncio.to_thread(lambda: tuple(_regenerate_chunks(value = data)))

                            assert _Context.writer is True
                            _Context.writer = False

                            try:
                                for data in data: writer.write(data)
                                await writer.drain()

                            except BaseException:
                                _Context.writer = None
                                raise

                            else: _Context.writer = True

                    _session = await asyncio.to_thread(lambda: _make_session(peer = _Peer(), shell = shell))
                    await _session()

                finally: writer.close()

            except BaseException:
                print(traceback.format_exc(), file = sys.stderr, flush = True)
                raise

        def _result(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
            return asyncio.gather(_session_coroutine(reader = reader, writer = writer), return_exceptions = True)

        return _result

    async def _stdio_session_coroutine(shell: _Shell):
        assert isinstance(shell, _Shell)

        async with (
            _open_stream_reader(source = sys.stdin) as _reader,
            _open_stream_writer(destination = sys.stdout) as _writer
        ):
            await asyncio.to_thread(_close_stdio)

            class _Context(object):
                reader = True
                writer = True

            class _Peer(_SessionPeer):
                @staticmethod
                async def read(size: int = None):
                    if size is not None:
                        assert isinstance(size, int)
                        assert 0 < size

                    assert _Context.reader is True
                    _Context.reader = False

                    try:
                        _data = await _reader(size = size)
                        assert isinstance(_data, bytes)

                    except BaseException:
                        _Context.reader = None
                        raise

                    else: _Context.reader = True if _data else None

                    return _data

                @staticmethod
                async def write(data: typing.Iterable[bytes]):
                    data = await asyncio.to_thread(lambda: tuple(_regenerate_chunks(value = data)))

                    assert _Context.writer is True
                    _Context.writer = False

                    try:
                        for data in data:
                            _size = await _writer(data)
                            assert isinstance(_size, int)
                            assert _size == len(data)

                    except BaseException:
                        _Context.writer = None
                        raise

                    else: _Context.writer = True

            _session = await asyncio.to_thread(lambda: _make_session(peer = _Peer(), shell = shell))
            await _session()

    async def _coroutine(listen: typing.Optional[dict], shell: typing.Optional[typing.Iterable[str]]):
        shell = await _make_shell(command = shell)

        if listen is None: await _stdio_session_coroutine(shell = shell)
        else:
            await asyncio.to_thread(_close_stdio)
            await _listener_coroutine(config = listen, delegate = _make_listener_delegate(shell = shell))

    def _routine(*args, **kwargs): asyncio.run(_coroutine(*args, **kwargs))

    class _Result(object):
        routine = _routine

    return _Result


_private = _private()
try: routine = _private.routine
finally: del _private
