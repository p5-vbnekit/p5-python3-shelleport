#!/usr/bin/env python3
# -*- coding: utf-8 -*-

assert "__main__" != __name__


def _private():
    import typing

    class _Class(object):
        @property
        def keys(self): return self.__dictionary.keys()

        def get(self, key: str):
            assert isinstance(key, str)
            return self.__dictionary[key]

        def set(self, key: str, validator: typing.Callable):
            assert isinstance(key, str) and bool(key)
            if validator is None: self.__dictionary.pop(key)
            assert callable(validator)
            self.__dictionary[key] = validator

        def decorator(self, key: str):
            assert isinstance(key, str) and bool(key)

            # noinspection PyShadowingNames
            def _decorator(validator: typing.Callable):
                # noinspection PyShadowingNames
                def _routine(*args, **kwargs):
                    try: return validator(*args, **kwargs)
                    except BaseException: raise ValueError(key)

                self.set(key = key, validator = _routine)

            return _decorator

        def __call__(self, arguments: dict, allow_unknown = False):
            assert isinstance(arguments, dict)
            assert isinstance(allow_unknown, bool)
            _keys = set(self.__dictionary.keys()).copy()
            for _key, _value in arguments.items():
                try: _validator = self.__dictionary[_key]
                except KeyError:
                    if allow_unknown: continue
                    raise
                arguments[_key] = _validator(value = _value)
                _keys.remove(_key)
            try: assert not _keys
            except AssertionError: raise ValueError(f"keys not found: {_keys}")

        def __init__(self):
            super().__init__()
            self.__dictionary = dict()

    class _Result(object):
        Class = _Class

    return _Result


_private = _private()
try: Class = _private.Class
finally: del _private


# noinspection PyArgumentList
def make(*args, **kwargs): return Class(*args, **kwargs)
