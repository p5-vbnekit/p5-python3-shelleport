#!/usr/bin/env python3
# -*- coding: utf-8 -*-

assert "__main__" != __name__


def _private():
    import argparse

    from .. import _common as _common_module

    _validator = _common_module.cli_validator.make()
    _platform_info = _common_module.platform_info.make()

    _backend = argparse.ArgumentParser(
        prog = _platform_info.program,
        description = "client/server shell teleport",
        exit_on_error = False
    )

    _subparsers = _backend.add_subparsers(
        title = "action", required = True, dest = "action"
    )

    class _Class(object):
        @property
        def subparsers(self): return _subparsers

        @staticmethod
        def parse(*args, **kwargs):
            _known, _unknown = _backend.parse_known_args(*args, **kwargs)
            if _unknown: raise ValueError("unrecognized arguments: %s" % " ".join(_unknown))
            _known = vars(_known)
            _validator(arguments = _known, allow_unknown = True)
            return _known

        @staticmethod
        def help(): return _backend.format_help()

    class _Result(object):
        Class = _Class

    return _Result


_private = _private()
try: Class = _private.Class
finally: del _private


# noinspection PyArgumentList
def make(*args, **kwargs): return Class(*args, **kwargs)
