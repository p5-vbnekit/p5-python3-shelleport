#!/usr/bin/env python3
# -*- coding: utf-8 -*-

assert "__main__" != __name__


def _private():
    import os
    import typing
    import argparse

    from . import _logic as _logic_module
    from ... import _common as _common_module

    _name = __package__.split(".")[-1]
    _name = _name.replace("_", "-")

    _parse_address = _common_module.parse_address
    _make_cli_validator = _common_module.cli_validator.make

    _routine = _logic_module.routine

    class _Class(_common_module.Mode):
        @property
        def name(self) -> str: return _name

        def setup_cli(self, parser: argparse.ArgumentParser):
            assert isinstance(parser, argparse.ArgumentParser)

            # noinspection PyShadowingNames
            @self.__cli_validator.decorator(key = parser.add_argument(
                "-e", "--export", action = "append", help = "export variables",
                dest = f"{self.name}/export", metavar = "EXPORT"
            ).dest)
            def _routine(value: typing.Optional[typing.List[str]]):
                if value is None: return tuple()
                assert isinstance(value, list)
                for _variable in value:
                    try:
                        assert isinstance(_variable, str)
                        assert _variable
                        assert _variable.lstrip() == _variable
                        try: _key, _value = _variable.split("=")
                        except ValueError: _key, _value = _variable, None
                        assert _key.strip() == _key
                        assert 1 == len(_key.splitlines())
                        if _value is None: assert _key in os.environ
                    except Exception: raise ValueError(_variable)
                return tuple(value)

            # noinspection PyShadowingNames
            @self.__cli_validator.decorator(key = parser.add_argument(
                nargs = 1, help = "peer address",
                dest = f"{self.name}/peer", metavar = "PEER"
            ).dest)
            def _routine(value: typing.List[str]):  # noqa: F811
                assert isinstance(value, list)
                value, = value
                value = _parse_address(value = value, scheme = "unix")
                if "tcp" == value["type"]: assert isinstance(value["port"], int)
                return value

            # noinspection PyShadowingNames
            @self.__cli_validator.decorator(key = parser.add_argument(
                nargs = "*", help = "arguments",
                dest = f"{self.name}/arguments", metavar = "-- ARGUMENTS"
            ).dest)
            def _routine(value: typing.List[str]):  # noqa: F811
                if value is None: return tuple()
                assert isinstance(value, list)
                for _item in value: assert isinstance(_item, str)
                return tuple(value)

        def validate_cli(self, arguments: dict):
            assert isinstance(arguments, dict)
            self.__cli_validator(arguments, allow_unknown = True)

        def __call__(self, cli: dict):
            assert isinstance(cli, dict)
            _peer = cli[f"{self.name}/peer"]
            _export = cli[f"{self.name}/export"]
            _arguments = cli[f"{self.name}/arguments"]
            _routine(peer = _peer, export = _export, arguments = _arguments)

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
