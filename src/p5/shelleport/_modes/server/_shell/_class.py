#!/usr/bin/env python3
# -*- coding: utf-8 -*-

assert "__main__" != __name__


def _private():
    import typing
    import asyncio

    from . import session as _session_module

    _Session = _session_module.Class

    _make_session = _session_module.make

    _validate_command = _Session.validate_command
    _validate_arguments = _Session.validate_arguments
    _get_default_command = _Session.get_default_command
    _validate_environment = _Session.validate_environment

    def _generate_session_command(head: typing.Iterable[str], tail: typing.Iterable[str] = None):
        yield from _validate_command(value = head)
        if tail is None: return
        yield from _validate_arguments(value = tail)

    class _Class(object):
        @property
        def command(self): return self.__command

        @property
        def environment(self): return None if self.__environment is None else self.__environment.copy()

        @staticmethod
        def get_default_command(): return _get_default_command()

        @staticmethod
        def validate_command(value: typing.Iterable[str]): return _validate_command(value = value)

        @staticmethod
        def validate_arguments(value: typing.Iterable[str]): return _validate_arguments(value = value)

        @staticmethod
        def validate_environment(value: typing.Dict[str, str]): return _validate_environment(value = value)

        def make_session(
            self, arguments: typing.Iterable[str] = None, environment: typing.Dict[str, str] = None
        ): return _make_session(
            command = _generate_session_command(head = self.__command, tail = arguments),
            environment = environment
        )

        async def __call__(self, arguments: typing.Iterable[str] = None, environment: typing.Dict[str, str] = None):
            _session = await asyncio.to_thread(lambda: self.make_session(
                arguments = arguments, environment = environment
            ))
            await _session.open()
            return _session

        def __init__(self, command: typing.Iterable[str], environment: typing.Dict[str, str] = None):
            super().__init__()
            command = _validate_command(value = command)
            if environment is not None: environment = _validate_environment(value = environment)
            self.__command = command
            self.__environment = environment

    class _Result(object):
        Class = _Class

    return _Result


_private = _private()
try: Class = _private.Class
finally: del _private


# noinspection PyArgumentList
def make(*args, **kwargs): return Class(*args, **kwargs)
