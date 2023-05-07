#!/usr/bin/env python3
# -*- coding: utf-8 -*-

if "__main__" != __name__:
    def _private():
        import sys
        import importlib

        _this_module = sys.modules[__name__]
        def _routine(*args, **kwargs): return importlib.import_module(*args, **kwargs)

        class _Callable(_this_module.__class__):
            def __call__(self, *args, **kwargs): return _routine(*args, **kwargs)

        _this_module.__class__ = _Callable

        class _Result(object):
            routine = _routine

        return _Result

    _private = _private()
    routine = _private.routine
    del _private
