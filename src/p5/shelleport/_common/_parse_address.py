#!/usr/bin/env python3
# -*- coding: utf-8 -*-

assert "__main__" != __name__


def _private():
    import pathlib
    import urllib.parse

    _tcp_scheme = "tcp://"
    _unix_scheme = "unix://"

    def _parse_tcp(value: str):
        assert value
        assert value.strip() == value
        if (not value.startswith("[")) and (1 < value.count(":")): value = f"[{value}]"
        value = urllib.parse.urlsplit(url = f"scheme://{value}", allow_fragments = False)
        assert "scheme" == value.scheme
        assert value.netloc
        assert value.path in {"", "/"}
        assert not value.query
        assert not value.fragment
        assert not value.username
        assert not value.password
        _host, _port = value.hostname, value.port
        assert _host
        if _port is not None:
            assert isinstance(_port, int)
            assert 0 < _port
            assert 65536 > _port
        return {"type": "tcp", "host": _host, "port": _port}

    def _parse_unix(value: str):
        assert value
        assert not value.endswith("/")
        assert not value.endswith("/.")
        assert not value.endswith("/..")
        return {"type": "unix", "path": pathlib.PurePosixPath(value).as_posix()}

    def _routine(value: str, scheme: str = None):
        assert isinstance(value, str)
        if scheme is not None:
            assert isinstance(scheme, str)
            assert scheme in {"tcp", "unix"}
        assert value
        value, = f"{value}\r\n".splitlines()
        if value.startswith(_tcp_scheme): return _parse_tcp(value = value[len(_tcp_scheme):])
        if value.startswith(_unix_scheme): return _parse_unix(
            value = urllib.parse.unquote(value[len(_unix_scheme):], errors = "strict")
        )
        assert scheme
        if "tcp" == scheme: return _parse_tcp(value = value)
        assert "unix" == scheme
        return _parse_unix(value = value)

    class _Result(object):
        routine = _routine

    return _Result


_private = _private()
try: routine = _private.routine
finally: del _private
