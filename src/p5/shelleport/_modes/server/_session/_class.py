#!/usr/bin/env python3
# -*- coding: utf-8 -*-

assert "__main__" != __name__


def _private():
    import typing
    import asyncio
    import traceback

    from . import peer as _peer_module
    from .. import _shell as _shell_module
    from .... import _common as _common_module

    _Peer = _peer_module.Class
    _Shell = _shell_module.Class
    _ShellSession = _shell_module.Session
    _IterationController = _common_module.asynchronous_tools.IterationController

    _protocol = _common_module.protocol.make()
    _valid_channels = {"stdin", "stdout", "stderr"}
    _make_task_group = _common_module.asynchronous_tools.task_group.make

    def _parse_start_request(value: dict):
        assert isinstance(value, dict)
        value = value.copy()
        _arguments = value.pop("arguments")
        assert isinstance(_arguments, list)
        _arguments = _Shell.validate_arguments(value = _arguments)
        _environment = value.pop("environment")
        assert isinstance(_environment, dict)
        _environment = _Shell.validate_environment(value = _environment)
        assert not value, f"unknown keys: {value.keys()}"
        return _arguments, _environment

    class _Logic(object):
        @property
        def state(self): return self.__state

        async def __call__(self):
            assert self.__state is None
            self.__state = True

            try:
                _start_request = await _IterationController.anext(target = self.__reader)
                _start_request = await asyncio.to_thread(lambda: _parse_start_request(value = _start_request))
                _arguments, _environment = _start_request
                await self.__send_message({"accepted": True})

                async with (
                    await self.__shell(arguments = _arguments, environment = _environment) as _shell,
                    _make_task_group(lazy = True) as _task_group
                ):
                    try:
                        _writer_task = _task_group.spawn(awaitable = self.__writer_coroutine(shell = _shell))
                        _task_group.spawn(awaitable = self.__reader_coroutine(shell = _shell))
                        await _task_group.wait(return_when = asyncio.FIRST_COMPLETED)
                        assert _writer_task.done()
                        await _writer_task
                    finally: _task_group.cancel()

            except BaseException:
                await self.__send_message({"exception": traceback.format_exc()})
                raise

            finally: self.__state = False

            assert not self.__writer_exception

        def __init__(
            self,
            shell: _Shell,
            reader: typing.AsyncIterator[dict],
            writer: typing.Callable[[dict], typing.Awaitable]
        ):
            super().__init__()
            assert isinstance(shell, _Shell)
            self.__state = None
            self.__shell = shell
            self.__reader = reader
            self.__writer = writer
            self.__closed_by_peer = set()
            self.__closed_by_shell = set()
            self.__writer_exception = False

        async def __reader_coroutine(self, shell: _ShellSession):
            assert isinstance(shell, _ShellSession)
            async for _message in self.__reader:
                assert isinstance(_message, dict)
                _channel = _message.pop("channel")
                assert isinstance(_channel, str)
                assert _channel in _valid_channels
                _blob = None
                if "stdin" == _channel:
                    try: _blob = _message.pop("blob")
                    except KeyError: pass
                    else: assert isinstance(_blob, bytes)
                assert not _message, "invalid message"
                assert _channel not in self.__closed_by_peer
                if _blob is None:
                    self.__closed_by_peer.add(_channel)
                    await shell.close_channel(_channel)
                    continue
                if _channel in self.__closed_by_shell: continue
                await shell.write(data = _blob)

        async def __writer_coroutine(self, shell: _ShellSession):
            assert isinstance(shell, _ShellSession)

            _result = None

            async for _event in shell.generate_events():
                assert isinstance(_event, dict)
                assert _result is None, "unexpected event"
                try: _result = _event.pop("result")
                except KeyError: pass
                else:
                    assert isinstance(_result, int)
                    assert not _event, "invalid event"
                    continue
                _channel = _event.pop("channel")
                assert _channel in _valid_channels
                try: _blob = _event.pop("blob")
                except KeyError: _blob = None
                else:
                    assert "stdin" != _channel
                    assert isinstance(_blob, bytes)
                    assert _blob
                assert not _event, "invalid event"
                assert _channel not in self.__closed_by_shell
                _message = {"channel": _channel}
                if _blob is None: self.__closed_by_shell.add(_channel)
                else:
                    if _channel in self.__closed_by_peer: continue
                    _message["blob"] = _blob
                await self.__send_message(message = _message)

            assert isinstance(_result, int)
            await self.__send_message(message = {"result": _result})

        async def __send_message(self, message: dict):
            assert isinstance(message, dict)
            assert message
            if self.__writer_exception: return
            try: await self.__writer(message)
            except BaseException:
                self.__writer_exception = True
                raise

    class _Class(object):
        @property
        def peer(self): return self.__peer

        @property
        def shell(self): return self.__shell

        async def __call__(self):
            async with (
                _protocol.open_reader(source = self.__peer.read) as _protocol_reader,
                _protocol.open_writer(destination = self.__peer.write) as _protocol_writer
            ): await (await asyncio.to_thread(lambda: _Logic(
                shell = self.__shell, reader = _protocol_reader, writer = _protocol_writer
            )))()

        def __init__(self, peer: _Peer, shell: _Shell):
            super().__init__()
            assert isinstance(peer, _Peer)
            assert isinstance(shell, _Shell)
            self.__peer = peer
            self.__shell = shell

    class _Result(object):
        Class = _Class

    return _Result


_private = _private()
try: Class = _private.Class
finally: del _private


# noinspection PyArgumentList
def make(*args, **kwargs): return Class(*args, **kwargs)
