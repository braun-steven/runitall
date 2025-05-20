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
            name = "R"
        self.name = name

    def __repr__(self) -> str:
        return f"{self.name}-{self.label}"


def parse_resource_integer_string(
    resource_integer_string: str,
    repetitions: int = 1,
    resource_name: Optional[str] = None,
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
    return list(
        sorted(
            [Resource(r, name=resource_name) for r in resources], key=lambda r: r.label
        )
    )


def run_shell_command_with_resource(
    command: str, resource: "Resource", return_stdout: bool = False
) -> Optional[str]:
    """
    Runs a shell command, replacing a placeholder with the resource label,
    streams its standard output in real-time to a file, and optionally returns it.

    Args:
        command: Command to run. Must contain RESOURCE_PLACEHOLDER.
        resource: Resource object to use. Its label will replace the placeholder.
        return_stdout: If True, collect and return all stdout lines as a single string.

    Returns:
        Optional[str]: Concatenated stdout if return_stdout is True, else None.
    """

    assert (
        RESOURCE_PLACEHOLDER in command
    ), f"Resource placeholder '{RESOURCE_PLACEHOLDER}' was not present in command ({command}). Insert the placeholder manually."

    # Replace the placeholder with the actual resource label
    command_with_resource = command.replace(RESOURCE_PLACEHOLDER, str(resource.label))

    logger.info(
        f"[{resource}] Preparing to run command on resource <{resource}>:\n{command_with_resource}"
    )

    # Sanitize the command string to create a valid filename
    # Replace spaces, forward slashes, and backslashes with underscores
    sanitized_command_filename = re.sub(r"[ /\\]", "_", command_with_resource)
    # Further sanitize to remove characters that might be problematic in filenames
    sanitized_command_filename = re.sub(r"[^\w\.-_]", "", sanitized_command_filename)
    # Limit filename length if necessary (e.g., to 255 characters)
    max_filename_len = 200
    if len(sanitized_command_filename) > max_filename_len:
        # Take a part of the beginning and a hash of the full command to keep it somewhat unique
        # and identifiable, while avoiding excessive length.
        command_hash = str(hash(command_with_resource))[-8:]  # Last 8 chars of hash
        sanitized_command_filename = (
            f"{sanitized_command_filename[:max_filename_len-10]}_{command_hash}"
        )

    # Define the output directory and filename
    output_dir = Path("/tmp/runitall")
    output_file_path = output_dir / f"{sanitized_command_filename}.stdout.log"

    # Create the output directory if it doesn't exist
    try:
        output_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"[{resource}] STDOUT will be written to: {output_file_path}")
    except OSError as e:
        logger.error(
            f"[{resource}] Failed to create output directory {output_dir}: {e}"
        )
        # Decide how to handle this: raise error, or proceed without file logging for this command
        # For now, we'll proceed, and writing to the file will likely fail if dir creation failed.
        # Alternatively, one could raise an exception here.
        # raise # or return None, or some error indicator

    collected_stdout_lines: List[str] = []

    try:
        # Start the subprocess
        process = Popen(
            command_with_resource,
            shell=True,
            stdout=PIPE,
            stderr=PIPE,
            text=True,
            bufsize=1,
        )

        logger.info(
            f"[{resource}] Command started. Streaming STDOUT to {output_file_path}..."
        )

        # Open the output file for writing
        with open(output_file_path, "w", encoding="utf-8") as outfile:
            if process.stdout:
                for line in iter(process.stdout.readline, ""):
                    # Write line to file (it already includes newline if present from source)
                    outfile.write(line)
                    outfile.flush()  # Ensure the line is written immediately
                    if return_stdout:
                        collected_stdout_lines.append(
                            line.rstrip()
                        )  # rstrip for collection

        process.wait()

        stderr_output = ""
        if process.stderr:
            stderr_output = process.stderr.read().rstrip()

        if process.returncode != 0:
            logger.error(
                f"[{resource}] Command failed with return code {process.returncode}."
            )
            if stderr_output:
                logger.error(f"[{resource}] STDERR:\n{stderr_output}")
                # Also write stderr to a file for failed commands
                stderr_file_path = (
                    output_dir / f"{sanitized_command_filename}.stderr.log"
                )
                try:
                    with open(stderr_file_path, "w", encoding="utf-8") as errfile:
                        errfile.write(
                            stderr_output + "\n"
                        )  # Add newline for clarity if reading file
                    logger.info(
                        f"[{resource}] STDERR output written to: {stderr_file_path}"
                    )
                except Exception as e_err_write:
                    logger.error(
                        f"[{resource}] Failed to write STDERR to file {stderr_file_path}: {e_err_write}"
                    )

        else:
            logger.info(
                f"[{resource}] Command completed successfully (Return Code: {process.returncode}). STDOUT at {output_file_path}"
            )
            if stderr_output:
                logger.info(
                    f"[{resource}] STDERR (Note: command succeeded):\n{stderr_output}"
                )
                # Optionally write stderr to file even on success if it exists
                stderr_file_path = (
                    output_dir / f"{sanitized_command_filename}.stderr.log"
                )
                if stderr_output.strip():  # Only write if there's actual content
                    try:
                        with open(stderr_file_path, "w", encoding="utf-8") as errfile:
                            errfile.write(stderr_output + "\n")
                        logger.info(
                            f"[{resource}] Non-empty STDERR output (on success) written to: {stderr_file_path}"
                        )
                    except Exception as e_err_write:
                        logger.error(
                            f"[{resource}] Failed to write STDERR (on success) to file {stderr_file_path}: {e_err_write}"
                        )

        if return_stdout:
            return "\n".join(collected_stdout_lines)
        return None

    except FileNotFoundError:
        logger.error(
            f"[{resource}] Error: The command or shell for '{command_with_resource}' was not found."
        )
        raise
    except Exception as e:
        logger.error(
            f"[{resource}] An unexpected error occurred while running command '{command_with_resource}': {e}",
            exc_info=True,
        )
        raise


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
    def run(
        self, resources: queue.Queue, stop_event: threading.Event
    ):  # pragma: no cover
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

    def run(self, resources: queue.Queue, stop_event: threading.Event = None):
        if stop_event is None:
            stop_event = threading.Event()
        self.wait_for_dependencies(stop_event=stop_event)
        self.mark_running()
        resource = resources.get()
        logger.info(f"[{resource}] Working on")
        run_shell_command_with_resource(self.command, resource=resource)
        logger.info(f"[{resource}] Finished")

        # Mark resource task as done
        resources.task_done()

        # Set task state to finished
        self.mark_finished()

        # Put resource back into queue of available resources
        resources.put(resource)

    def __repr__(self):
        return f"Task({self.command}, state={self.state}, depends_on={self.depends_on})"


def _worker(
    task_queue: queue.Queue, resources: queue.Queue, stop_event: threading.Event
) -> None:
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


def run_tasks(
    tasks: List[Task], resources: List[Resource], wait_between_task_starts: int = 2
):
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
                target=_worker,
                daemon=False,
                args=(task_queue, resource_queue, stop_event),
            )
            time.sleep(wait_between_task_starts)
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


if __name__ == "__main__":
    # Configure basic logging for the example
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(name)s - %(threadName)s - %(message)s",
    )

    logger.info("Starting task execution example.")

    # Example command that prints numbers with a delay, using the resource placeholder
    # This command will show streaming output.
    cmd_stream = (
        f"echo 'Starting stream test for {RESOURCE_PLACEHOLDER}...'; "
        f"for i in $(seq 1 5); do "
        f"  echo 'Output line $i from {RESOURCE_PLACEHOLDER}'; "
        f"  sleep 0.5; "
        f"done; "
        f"echo 'Stream test for {RESOURCE_PLACEHOLDER} finished.'"
    )

    # Another command for a second task
    cmd_quick = f"echo 'Quick task on {RESOURCE_PLACEHOLDER} reporting in!'; sleep 1"

    # Create resources
    gpu_resources = parse_resource_integer_string("0,1", resource_name="GPU")
    # If you have only one resource for testing:
    # gpu_resources = [Resource(label=0, name="GPU")]

    # Define tasks
    task1 = ShellTask(
        command=cmd_stream,
    )

    task2 = ShellTask(
        command=cmd_quick, depends_on=[task1]  # Task 2 will wait for Task 1
    )

    task3_independent = ShellTask(
        command=f"echo 'Independent task on {RESOURCE_PLACEHOLDER} here!'; sleep 2"
    )

    all_tasks = [task1, task2, task3_independent]
    # For a simpler test with one task:
    # all_tasks = [task1]

    if not gpu_resources:
        logger.error(
            "No GPU resources parsed. Exiting. Check parse_resource_integer_string input."
        )
    elif not all_tasks:
        logger.error("No tasks defined. Exiting.")
    else:
        logger.info(
            f"Running {len(all_tasks)} tasks on {len(gpu_resources)} resources."
        )
        run_tasks(
            tasks=all_tasks,
            resources=gpu_resources,
            wait_between_task_starts=0.1,  # Small delay between starting threads
        )

    logger.info("Task execution example finished.")
