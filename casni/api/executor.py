import sys
import inspect
import threading
import asyncio
from typing import Union, Optional, Tuple, List, Dict, Callable, Awaitable, Coroutine, Any
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from functools import partial


# Functions
def is_list_of_tasks(lst: List):
    if not isinstance(lst, list):
        return False
    return all(isinstance(item, Task) for item in lst)


# DataClasses
@dataclass
class Task:
    function: Union[Callable, Awaitable]
    args: Union[Tuple, List, None] = None
    kwargs: Optional[Dict] = None
    desc: Optional[str] = None

    def partial(self):
        if asyncio.iscoroutinefunction(self.function):
            if self.args and self.kwargs:
                return self.function(*self.args, **self.kwargs)
            elif self.args and not self.kwargs:
                return self.function(*self.args)
            elif not self.args and self.kwargs:
                return self.function(**self.kwargs)
            else:
                return self.function()
                    
        elif callable(self.function):
            if self.args and self.kwargs:
                return partial(self.function, *self.args, **self.kwargs)
            elif self.args and not self.kwargs:
                return partial(self.function, *self.args)
            elif not self.args and self.kwargs:
                return partial(self.function, **self.kwargs)
            else:
                return partial(self.function)

    def __repr__(self):
        if self.desc:
            string = f'<Task: {self.desc}>'
        else:
            string = f"<Task: '{self.function.__name__}'>"
        return string


@dataclass
class Output:
    task: Task
    returned: Any
    
    def __repr__(self):
        return f"Output(task={str(self.task)}, returned={True if self.returned else False})"

@dataclass
class Terminator:
    pass


# MainClasses
class NIPexec(threading.Thread):
    """
    Method
    - submit
    - stop
    """
    def __init__(self, max_workers=4):
        super().__init__()
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        self._queue = asyncio.Queue()
        self.history = []
        self.loop = asyncio.new_event_loop()
        self._set_name()
        self.start()
    
    def _set_name(self):
        base_name = 'NIPexec'
        id = len([t for t in threading.enumerate() if base_name in t.name]) + 1
        self.name = f"{base_name}-{id}"

    def _exec_coroutine(self, func: Union[Callable, Coroutine]):
        result = asyncio.run_coroutine_threadsafe(func, self.loop)
        return result
    
    def run(self):
        """serve as a target for Thread
        so when start() method called, this will run
        which means the script in this method will run under the new Thread
        """
        self.loop.run_until_complete(self._event_loop())
    
    async def _event_loop(self):
        while True:
            task = await self._queue.get()
            if isinstance(task, str) and task == "stop":
                break
            elif isinstance(task, Task):
                if asyncio.iscoroutinefunction(task.function):
                    """input task is coroutine"""
                    future = await asyncio.wrap_future(self._exec_coroutine(task.partial()))
                elif callable(task.function):
                    """input task is typical function"""
                    future = await self.loop.run_in_executor(self.executor, task.partial())
            else:
                print(f"Invalid task: {str(task)}", file=sys.stderr)
            self.history.append(Output(task=task, returned=future))
    
    def submit(self, task: Union[Task, List[Task], Terminator, str]):
        """submit Task object or list of Task object to event loop
        """
        if isinstance(task, Task):
            task = [task]
        elif is_list_of_tasks(task):
            pass
        elif isinstance(task, Terminator):
            task = ["stop"]
        for t in task:
            self._exec_coroutine(self._queue.put(t))

    @property
    def queue(self):
        return self._queue._queue
    
    def set_max_workers(self, max_workers):
        self.executor._max_workers = max_workers

    def clear(self):
        while len(self.history):
            output = self.history.pop()
            del output

    def stop(self):
        # shutdown executor
        self.executor.shutdown()

        # shutdown event loop
        if self.loop.is_running():
            self._exec_coroutine(self._queue.put("stop"))

        # shutdown thread
        if self.is_alive():
            self.join()
            
    def __del__(self):
        self.stop()


if __name__ == "__main__":
    react = NIPexec()