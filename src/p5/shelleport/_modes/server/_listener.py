#!/usr/bin/env python3
# -*- coding: utf-8 -*-

assert "__main__" != __name__


def _private():
    import os
    import typing
    import asyncio
    import pathlib
    import ipaddress

    _delegate_type_hint = typing.Callable[[asyncio.StreamReader, asyncio.StreamWriter], typing.Awaitable]

    def _make_launchers():
        _collector = dict()

        def _make_decorator(mode: str):
            assert isinstance(mode, str)
            assert mode
            assert mode not in _collector
            def _result(delegate: typing.Callable): _collector[mode] = delegate
            return _result

        @_make_decorator(mode = "tcp")
        def _routine(address: str, port: int):
            assert isinstance(address, str)
            assert isinstance(port, int)
            if address:
                if "*" == address: address = None
                else: address = str(ipaddress.ip_address(address = address))
            else: address = None
            assert (0 < port) and (65536 > port)

            def _result(delegate: _delegate_type_hint): return asyncio.start_server(
                delegate, host = address, port = port
            )

            return _result

        @_make_decorator(mode = "unix")
        def _routine(path: str, access: int = 0o600):  # noqa: F811
            assert isinstance(path, str)
            assert isinstance(access, int)
            assert 0 <= access
            assert 0o1777 >= access

            path = pathlib.Path(path).resolve()
            os.makedirs(path.parent.as_posix(), exist_ok = True)
            path = path.as_posix()

            async def _result(delegate: _delegate_type_hint):
                _server = await asyncio.start_unix_server(delegate, path = path, start_serving = False)

                try: await asyncio.to_thread(lambda: os.chmod(path, access))
                except BaseException:
                    _server.close()
                    raise

                return _server

            return _result

        return _collector

    _launchers = _make_launchers()
    del _make_launchers

    async def _coroutine(
        config: typing.Dict[str, typing.Any],
        delegate: typing.Callable[[asyncio.StreamReader, asyncio.StreamWriter], typing.Awaitable]
    ):
        assert isinstance(config, dict)
        config = config.copy()
        _mode = config.pop("mode")
        assert isinstance(_mode, str)
        async with await _launchers[_mode](**config)(delegate = delegate) as _server: await _server.serve_forever()

    class _Result(object):
        coroutine = _coroutine

    return _Result


_private = _private()
try: coroutine = _private.coroutine
finally: del _private
