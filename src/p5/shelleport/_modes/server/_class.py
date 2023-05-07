#!/usr/bin/env python3
# -*- coding: utf-8 -*-

assert "__main__" != __name__


def _private():
    import re
    import typing
    import argparse
    import ipaddress

    from . import _logic as _logic_module
    from . import _shell as _shell_module
    from ... import _common as _common_module

    _name = __package__.split(".")[-1]
    _name = _name.replace("_", "-")

    _Shell = _shell_module.Class

    _routine = _logic_module.routine
    _parse_address = _common_module.parse_address
    _make_cli_validator = _common_module.cli_validator.make
    _unix_access_pattern = re.compile("^[0-1]?[0-7]{3}$")

    class _Class(_common_module.Mode):
        @property
        def name(self) -> str: return _name

        def setup_cli(self, parser: argparse.ArgumentParser):
            assert isinstance(parser, argparse.ArgumentParser)

            # noinspection PyShadowingNames
            @self.__cli_validator.decorator(key = parser.add_argument(
                "-l", "--listen", required = False,
                help = "listen address (`stdio://` as default, `[unix://path]` or `tcp://host[:port]`)",
                dest = f"{self.name}/listen", metavar = "PEER"
            ).dest)
            def _routine(value: typing.Optional[str]):
                if value is None: return None
                assert isinstance(value, str)
                assert value
                if "stdio://" == value: return None
                value = _parse_address(value = value, scheme = "unix")
                _mode = value.pop("type")
                if "unix" != _mode:
                    assert isinstance(value["port"], int)
                    _address = value.pop("host")
                    assert isinstance(_address, str)
                    if _address:
                        if "*" == _address: _address = ""
                        else: _address = str(ipaddress.ip_address(address = _address))
                    value["address"] = _address
                value["mode"] = _mode
                return value

            # noinspection PyShadowingNames
            @self.__cli_validator.decorator(key = parser.add_argument(
                "-m", "--unix-access", required = False,
                help = "unix socket access mode (octal)", dest = f"{self.name}/unix-access", metavar = "MODE"
            ).dest)
            def _routine(value: typing.Optional[str]):  # noqa: F811
                if value is None: return None
                assert isinstance(value, str)
                assert _unix_access_pattern.match(value) is not None
                return int(value, base = 8)

            # noinspection PyShadowingNames
            @self.__cli_validator.decorator(key = parser.add_argument(
                nargs = "*", help = "shell (and arguments)",
                dest = f"{self.name}/shell", metavar = "-- SHELL"
            ).dest)
            def _routine(value: typing.List[str]):  # noqa: F811
                assert isinstance(value, list)
                if value: return _Shell.validate_command(value = value)
                return None

        def validate_cli(self, arguments: dict):
            assert isinstance(arguments, dict)
            self.__cli_validator(arguments, allow_unknown = True)
            _unix_socket_access = arguments[f"{self.name}/unix-access"]
            if _unix_socket_access is not None:
                _listen = arguments[f"{self.name}/listen"]
                assert (_listen is not None) and "unix" == _listen["mode"], "unix mode listen expected"

        def __call__(self, cli: dict):
            assert isinstance(cli, dict)
            _shell = cli[f"{self.name}/shell"]
            _listen = cli[f"{self.name}/listen"]
            _unix_socket_access = cli[f"{self.name}/unix-access"]
            if _unix_socket_access is not None:
                assert "unix" == _listen["mode"]
                _listen["access"] = _unix_socket_access
            _routine(shell = _shell, listen = _listen)

        def __init__(self):
            super().__init__()
            self.__cli_validator = _make_cli_validator()

    class _Result(object):
        Class = _Class

    return _Result


_private = _private()
try: Class = _private.Class
finally: del _private


# noinspection PyArgumentList
def make(*args, **kwargs): return Class(*args, **kwargs)
