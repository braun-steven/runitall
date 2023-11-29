#!/usr/bin/env python3

import os
from enum import Enum
import queue
import argparse
import threading
import os
import time
from pathlib import Path
from typing import List, Union, Optional, Any
from abc import ABC, abstractmethod
import re
from subprocess import PIPE, Popen

RESOURCE_PLACEHOLDER = "<RESOURCE_TOKEN>"

import logging

logger = logging.getLogger(__name__)


def parse_resource_integer_string(
    resource_integer_string: str, repetitions: int = 1
) -> List["Resource"]:
    """
    Parse a comma-separated list of integers.


    E.g.:
    "1,2,3"  --> [1,2,3]
    "1,3-8,10"  --> [1,3,4,5,6,7,8,10]
    "1,3:8,10"  --> [1,3,4,5,6,7,8,10]

    Args:
        resource_integer_string (str): Comma-separated list of integers.
        repetitions (int): Number of repetitions for each resource. Useful for small experiments where a resource is not fully utilized.

    Returns:
        List[Resource]: List of resources.
    """
    resources = []
    for tag in resource_integer_string.split(","):
        # Check if tag is a range
        if re.search("[:-]", tag):
            # Parse range
            lower, upper = re.split("[:-]", tag)
            lower, upper = int(lower), int(upper)

            # Add range to list
            for x in range(lower, upper + 1):
                resources.append(str(x))
        else:
            # Add single resource to list
            resources.append(tag)
    resources = list(set(resources))  # Ensure unique resources
    resources = resources * repetitions  # Repeat resources
    return list(sorted([Resource(r) for r in resources], key=lambda r: r.label))


def run_shell_command_with_resource(
    command: str, resource: "Resource", return_stdout: bool = False
) -> Optional[str]:
    """

    Args:
      command: Comand to run.
      resource: Resource to use.
      return_stdout: If True, return stdout of command.

    Returns:
        Optional[str]: Stdout of command if return_stdout is True, else None.

    """
    assert (
        RESOURCE_PLACEHOLDER in command
    ), f"Resource placeholder '{RESOURCE_PLACEHOLDER}' was not present in command ({command}). Insert the placeholder manually or use 'from runitall import RESOURCE_PLACEHOLDER'."
    command = command.replace(RESOURCE_PLACEHOLDER, str(resource.label))
    logger.info(f"Running command on resource <{resource}> " + command)
    p = Popen(command, shell=True, stdout=PIPE, stderr=PIPE)
    stdout, stderr = p.communicate()
    if return_stdout:
        return stdout.decode("utf-8")


class Resource:
    """Simple class to represent a resource (e.g. GPU, CPU, PORT, etc.)."""

    def __init__(self, label: Union[str, int], name: Optional[str] = None) -> None:
        """
        Args:
            label: Label/id of the resource.
            name: Name of the resource.
        """
        self.label = label

        # If no name is given, use "Resource" as default name
        if name is None:
            name = "Resource"
        self.name = name

    def __repr__(self) -> str:
        return f"{self.name}({self.label})"


class TaskState(Enum):
    """Enum for the state of a task."""

    WAITING = 0  # Task is waiting to be executed
    RUNNING = 1  # Task is currently running
    FINISHED = 2  # Task is finished


class Task(ABC):
    def __init__(self, depends_on: Optional[Union["Task", List["Task"]]] = None):
        """
        Args:
            depends_on: Task(s) that must be finished before this task can be executed.
        """
        self.state = TaskState.WAITING

        # If depends_on is a single task, wrap it in a list
        if depends_on is None:
            depends_on = []
        elif type(depends_on) == ShellTask:
            depends_on = [depends_on]
        self.depends_on: List[ShellTask] = depends_on

    def check_all_dependencies_finished(self) -> bool:
        return all(
            [dependency.state == TaskState.FINISHED for dependency in self.depends_on]
        )

    def wait_for_dependencies(self, sleep_time=10, stop_event: threading.Event = None):
        """Check if all dependencies are finished.

        If not, wait for a given amount of time and check again.

        Args:
            sleep_time: Time to wait between checks.
        """
        # If No dependency is given, return immediately
        if len(self.depends_on) == 0:
            return

        # If dependencies are given, check if state is finished
        while not self.check_all_dependencies_finished():

            if stop_event.is_set():
                logger.error("Received keyboard interrupt, quitting threads.")
                raise KeyboardInterrupt

            logger.info(
                f"Trying to run {self} but dependence on tasks {self.depends_on} is not fulfilled yet. Sleeping another 10 seconds."
            )
            # if not finished, wait for 10 seconds and try again
            time.sleep(sleep_time)

        # Finally return so that code after check_dependency can run
        return

    def mark_finished(self):
        """Mark task as finished."""
        self.state = TaskState.FINISHED

    def mark_running(self):
        """Mark task as running."""
        self.state = TaskState.RUNNING

    @abstractmethod
    def run(self, resources: queue.Queue, stop_event: threading.Event):  # pragma: no cover
        """
        Run the task.
        A list of resources is given instead of a single resource since:
        a. Some tasks might require multiple resources
        b. Resources should only be reserved when the task dependcy check is passed.

        Args:
            resources: Queue of resources to use.
            stop_event: Event to signal threads to stop.
        """
        pass


class ShellTask(Task):
    """
    Class for running multiple a shell task. Supports dependencies on other tasks.
    """

    def __init__(
        self,
        command,
        depends_on: Optional[Union["ShellTask", List["ShellTask"]]] = None,
    ) -> None:
        """
        Create a task
        Args:
            command: Command to run.
            depends_on: Task(s) that must be finished before this task can be run.
        """
        super().__init__(depends_on=depends_on)

        # Command to run
        self.command = command

    def run(self, resources: queue.Queue, stop_event: threading.Event=None):
        if stop_event is None:
            stop_event = threading.Event()
        self.wait_for_dependencies(stop_event=stop_event)
        self.mark_running()
        resource = resources.get()
        logger.info(f"Working on {resource}")
        run_shell_command_with_resource(self.command, resource=resource)
        logger.info(f"Finished on {resource}")

        # Mark resource task as done
        resources.task_done()

        # Set task state to finished
        self.mark_finished()

        # Put resource back into queue of available resources
        resources.put(resource)

    def __repr__(self):
        return f"Task({self.command}, state={self.state}, depends_on={self.depends_on})"


def _worker(task_queue: queue.Queue, resources: queue.Queue, stop_event: threading.Event) -> None:
    """
    Thread worker method. Takes a task from the task queue and runs it with the resource queue.

    Args:
        task_queue (queue.Queue): Queue of tasks.
        resources (queue.Queue): Queue of resources.
    """
    # Run task with queue of resources
    task = task_queue.get()
    task.run(resources=resources, stop_event=stop_event)
    task_queue.task_done()


def _make_queue(objects: List[Any]) -> queue.Queue:
    """Create a queue from a list of objects."""
    q = queue.Queue()
    for obj in objects:
        q.put(obj)
    return q


def run_tasks(tasks: List[Task], resources: List[Resource]):
    """
    Run a list of tasks on a list of resources.

    Args:
      tasks (List[Task]): List of tasks to run.
      resources (List[Resource]): List of resources to use.
    """
    # Create queues
    resource_queue = _make_queue(resources)
    task_queue = _make_queue(tasks)

    logger.info(f"Starting {len(tasks)} tasks on {len(resources)} resources.")

    # Create a stop event for worker threads
    stop_event = threading.Event()

    # For each task start a worker thread
    try:
        for _ in tasks:
            t = threading.Thread(
                target=_worker, daemon=False, args=(task_queue, resource_queue, stop_event)
            )
            time.sleep(2)
            t.start()

        task_queue.join()
        logger.info("All tasks finished.")
    except (KeyboardInterrupt, SystemExit):
        logger.error("Received keyboard interrupt, quitting threads.")
        stop_event.set()  # Signal all threads to exit

        # Optionally, wait for all threads to finish
        for t in threading.enumerate():
            if t is not threading.current_thread():
                t.join()
