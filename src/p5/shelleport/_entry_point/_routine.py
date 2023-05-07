#!/usr/bin/env python3
# -*- coding: utf-8 -*-

assert "__main__" != __name__


def _private():
    import sys
    import atexit

    from . import _cli_parser as _cli_parser_module

    from .. import _modes as _modes_module
    from .. import _common as _common_module

    _platform_info = _common_module.platform_info.make()

    def _generate_modes():
        for _module in (
            _modes_module.client,
            _modes_module.server
        ): yield _module.make()

    _modes = {_action.name: _action for _action in _generate_modes()}

    def _make_cli_parser():
        _root = _cli_parser_module.make()
        _subparsers = _root.subparsers
        for _name, _action in _modes.items(): _action.setup_cli(parser = _subparsers.add_parser(_name))
        return _root

    def _routine():
        _cli_parser = _make_cli_parser()
        _help_message = _cli_parser.help()

        try:
            _parsed_cli = _cli_parser.parse()
            _modes[_parsed_cli["action"]].validate_cli(arguments = _parsed_cli)

        except BaseException as _exception:
            if all((
                _platform_info.tty,
                (not isinstance(_exception, SystemExit)) or (0 != _exception.code)
            )): atexit.register(lambda: print(_help_message, flush = True, file = sys.stderr))
            raise

        del _cli_parser
        _modes[_parsed_cli["action"]](cli = _parsed_cli)

    class _Result(object):
        routine = _routine

    return _Result


_private = _private()
try: routine = _private.routine
finally: del _private
