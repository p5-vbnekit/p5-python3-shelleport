#!/usr/bin/env python3
# -*- coding: utf-8 -*-

assert "__main__" != __name__


def _private():
    import typing

    class _Class(object):
        @staticmethod
        async def read(size: int = None):
            assert (size is None) or isinstance(size, int)
            raise NotImplementedError()

        @staticmethod
        async def write(data: typing.Iterable[bytes]):
            for data in data: assert isinstance(data, bytes)
            raise NotImplementedError()

    class _Result(object):
        Class = _Class

    return _Result


_private = _private()
try: Class = _private.Class
finally: del _private


# noinspection PyArgumentList
def make(*args, **kwargs): return Class(*args, **kwargs)
