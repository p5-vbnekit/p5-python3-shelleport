#!/usr/bin/env python3
# -*- coding: utf-8 -*-

assert "__main__" != __name__


def _private():
    import os
    import typing
    import shutil
    import asyncio
    import pathlib
    import contextlib

    from . import _pipe as _pipe_module
    from . import _subprocess_cleaner as _subprocess_cleaner_module
    from .... import _common as _common_module

    _Pipe = _pipe_module.Class
    _IterationController = _common_module.asynchronous_tools.IterationController

    _make_pipe = _pipe_module.make
    _make_task_group = _common_module.asynchronous_tools.task_group.make
    _default_environment = os.environ.copy()
    _make_subprocess_cleaner = _subprocess_cleaner_module.make

    def _validate_arguments(value: typing.Iterable[str]):
        value = tuple(value)
        for _argument in value: assert isinstance(_argument, str)
        return value

    def _validate_command(value: typing.Iterable[str]):
        _iterator = iter(value)
        try: value = next(_iterator)
        except StopIteration: raise ValueError("empty command")
        assert isinstance(value, str)
        value, = f"{value}\r\n".splitlines()
        assert value, "empty command"
        value = pathlib.Path(value)
        if 1 < len(value.parts): value = value.absolute().as_posix()
        else:
            value, = value.parts
            assert value
            value = shutil.which(str(value))
            assert isinstance(value, str)
            assert value
        return value, *_validate_arguments(value = _iterator)

    def _validate_environment(value: typing.Dict[str, str]):
        assert isinstance(value, dict)
        for _key, _value in value.items():
            assert isinstance(_key, str)
            assert isinstance(_value, str)
            assert _key
            _key, = f"{_key}\r\n".splitlines()
        return value

    def _rebuild_environment(value: typing.Dict[str, str] = None):
        _result = _default_environment.copy()
        if value is not None: _result.update(_validate_environment(value = value))
        return _result

    async def _get_default_command():
        async with _make_subprocess_cleaner(subprocess = await asyncio.create_subprocess_shell(
            "which \"$0\"", stdout = asyncio.subprocess.PIPE
        )) as _subprocess:
            _subprocess = _subprocess.subprocess
            await asyncio.wait_for(_subprocess.wait(), timeout=+3.0e+0)
            assert 0 == _subprocess.returncode
            _response = await _subprocess.stdout.read()

        def _parse():
            assert isinstance(_response, bytes)
            assert _response
            _result = _response.decode("utf-8").strip()
            assert _result
            return pathlib.Path(_result).resolve(strict = True).as_posix()

        return (await asyncio.to_thread(_parse), )

    @contextlib.asynccontextmanager
    async def _open_pipe(mode: str):
        async with await asyncio.to_thread(lambda: _make_pipe(mode = mode)) as _pipe:
            await _pipe.open()
            yield _pipe

    async def _read_pipe_coroutine(channel: str, pipe: _Pipe, queue: asyncio.Queue):
        assert isinstance(channel, str)
        assert isinstance(pipe, _Pipe)
        assert isinstance(queue, asyncio.Queue)
        assert channel
        while True:
            _chunk = await pipe.read()
            assert isinstance(_chunk, bytes)
            if not _chunk: break
            await queue.put({"channel": channel, "blob": _chunk})
        await queue.put({"channel": channel})

    async def _read_pipes_coroutine(pipes: typing.Iterable[typing.Tuple[str, _Pipe]], queue: asyncio.Queue):
        assert isinstance(queue, asyncio.Queue)

        async with _make_task_group(lazy = True) as _task_group:
            try: await asyncio.wait([
                _task_group.spawn(awaitable = _read_pipe_coroutine(channel = _channel, pipe = _pipe, queue = queue))
                for _channel, _pipe in pipes
            ], return_when = asyncio.FIRST_EXCEPTION)
            finally: _task_group.cancel()

    @contextlib.asynccontextmanager
    async def _open_manipulator(command: typing.Iterable[str], environment: typing.Dict[str, str]):
        class _Manipulator(object): pass

        _queue = asyncio.Queue(maxsize = 1)

        async with (
            _open_pipe(mode = "w") as _stdin,
            _open_pipe(mode = "r") as _stdout,
            _open_pipe(mode = "r") as _stderr
        ):
            async with _make_subprocess_cleaner(subprocess = await asyncio.create_subprocess_exec(
                *command, env = environment,
                stdin = _stdin.remote.descriptor,
                stdout = _stdout.remote.descriptor,
                stderr = _stderr.remote.descriptor
            )) as _subprocess:
                _subprocess = _subprocess.subprocess

                _Manipulator.write = _stdin.write

                _pipes = {"stdin": _stdin, "stdout": _stdout, "stderr": _stderr}

                async def _close_remote_pipes():
                    async with _make_task_group(lazy = True) as _task_group:
                        try: await asyncio.wait([
                            _task_group.spawn(awaitable = _pipe.remote.close()) for _pipe in _pipes.values()
                        ], return_when = asyncio.FIRST_EXCEPTION)
                        finally: _task_group.cancel()

                await _close_remote_pipes()
                del _close_remote_pipes

                async def _close_channel(key: str):
                    assert isinstance(key, str)
                    _pipe = _pipes[key]
                    if _pipe.state: await _pipe.close()

                _Manipulator.close_channel = _close_channel
                del _close_channel

                async def _generator():
                    _read_task = asyncio.create_task(_read_pipes_coroutine(
                        pipes = [(_channel, _pipe) for _channel, _pipe in _pipes.items() if "r" == _pipe.mode],
                        queue = _queue
                    ))

                    try:
                        while not _read_task.done():
                            _queue_task = asyncio.create_task(_queue.get())

                            try:
                                await asyncio.wait((_read_task, _queue_task), return_when = asyncio.FIRST_COMPLETED)
                                if not _queue_task.done(): break
                                _message = await _queue_task
                                assert isinstance(_message, dict)
                                yield _message

                            finally:
                                if not _queue_task.done(): _queue_task.cancel()
                                await asyncio.gather(_queue_task, return_exceptions = True)

                        assert _read_task.done()
                        await _read_task

                        await _subprocess.wait()
                        _exit_code = _subprocess.returncode
                        assert isinstance(_exit_code, int)
                        yield {"result": _exit_code}

                    finally:
                        if not _read_task.done(): _read_task.cancel()
                        await asyncio.gather(_read_task, return_exceptions = True)

                _generator = _generator()

                try:
                    _Manipulator.iterator = _IterationController.aiter(target = _generator)
                    yield _Manipulator

                finally: await _generator.aclose()

    class _Class(object):
        @property
        def state(self): return self.__state

        @property
        def command(self): return self.__command

        @property
        def environment(self): return self.__environment.copy()

        @staticmethod
        def get_default_command(): return _get_default_command()

        @staticmethod
        def validate_command(value: typing.Iterable[str]): return _validate_command(value = value)

        @staticmethod
        def validate_arguments(value: typing.Iterable[str]): return _validate_arguments(value = value)

        @staticmethod
        def validate_environment(value: typing.Dict[str, str]): return _validate_environment(value = value)

        async def open(self):
            assert self.__state is None
            self.__state = False
            assert self.__context is None
            _context = _open_manipulator(command = self.__command, environment = self.__environment)
            _manipulator = await _context.__aenter__()
            self.__state = True
            self.__context = _context
            self.__manipulator = _manipulator

        async def close(self):
            assert self.__state is True
            self.__state = False
            _context = self.__context
            assert _context is not None
            self.__context = None
            await _context.__aexit__(None, None, None)

        async def write(self, data: bytes):
            assert isinstance(data, bytes)
            assert data
            _manipulator = self.__manipulator
            assert _manipulator is not None, "not opened"
            return await _manipulator.write(data = data)

        async def generate_events(self):
            _manipulator = self.__manipulator
            assert _manipulator is not None, "not opened"
            _iterator = _manipulator.iterator
            _manipulator.iterator = None
            assert _iterator is not None
            async for _event in _iterator: yield _event

        async def close_channel(self, key: str):
            assert isinstance(key, str)
            assert key
            _manipulator = self.__manipulator
            assert _manipulator is not None, "not opened"
            return await _manipulator.close_channel(key = key)

        async def __aenter__(self): return self

        async def __aexit__(self, exception_type, exception_instance, exception_traceback):
            if self.__state is not True: return
            self.__state = False
            _context = self.__context
            assert _context is not None
            self.__context = None
            await _context.__aexit__(exception_type, exception_instance, exception_traceback)

        def __init__(self, command: typing.Iterable[str], environment: typing.Dict[str, str] = None):
            super().__init__()
            command = _validate_command(value = command)
            environment = _rebuild_environment(value = environment)
            self.__state = None
            self.__context = None
            self.__command = command
            self.__manipulator = None
            self.__environment = environment

    class _Result(object):
        Class = _Class

    return _Result


_private = _private()
try: Class = _private.Class
finally: del _private


# noinspection PyArgumentList
def make(*args, **kwargs): return Class(*args, **kwargs)
