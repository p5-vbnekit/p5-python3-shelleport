#!/usr/bin/env python3
# -*- coding: utf-8 -*-

assert "__main__" != __name__


def _private():
    import argparse

    class _Class(object):
        @property
        def name(self) -> str: raise NotImplementedError()

        @staticmethod
        def setup_cli(parser: argparse.ArgumentParser): assert isinstance(parser, argparse.ArgumentParser)

        @staticmethod
        def validate_cli(arguments: dict): assert isinstance(arguments, dict)

        def __call__(self, cli: dict): assert isinstance(cli, dict)

    class _Result(object):
        Class = _Class

    return _Result


_private = _private()
try: Class = _private.Class
finally: del _private
