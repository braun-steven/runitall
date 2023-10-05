#!/usr/bin/env python3

from runitall import run_tasks
import time
import queue
import unittest

from runitall.runitall import (
    run_shell_command_with_resource,
    parse_resource_integer_string,
    RESOURCE_PLACEHOLDER,
    Resource,
    ShellTask, TaskState,
)


class TestApp(unittest.TestCase):
    def test_run_shell_command_with_resource(self) -> None:
        resource_label = 0
        resource = Resource(name="test-resource", label=resource_label)
        command = f"echo 'Hello World from {RESOURCE_PLACEHOLDER}'"
        out = run_shell_command_with_resource(command, resource, return_stdout=True)
        self.assertEqual(out, f"Hello World from {resource_label}\n")

    def test_parse_resource_integer_string_single(self) -> None:
        string = "1"
        resources = parse_resource_integer_string(string)
        self.assertEqual(len(resources), 1)
        self.assertEqual(resources[0].label, string)

    def test_parse_resource_integer_string_two(self) -> None:
        string = "1,3"
        resources = parse_resource_integer_string(string)
        self.assertEqual(len(resources), 2)
        self.assertEqual(resources[0].label, "1")
        self.assertEqual(resources[1].label, "3")

    def test_parse_resource_integer_string_multiple(self) -> None:
        for string in ["1-3", "1:3"]:
            resources = parse_resource_integer_string(string)
            self.assertEqual(len(resources), 3)
            self.assertEqual(resources[0].label, "1")
            self.assertEqual(resources[1].label, "2")
            self.assertEqual(resources[2].label, "3")

    def test_parse_resource_integer_string_multiple_colon_and_comma(self) -> None:
        string = "1:3,7"
        resources = parse_resource_integer_string(string)
        self.assertEqual(len(resources), 4)
        self.assertEqual(resources[0].label, "1")
        self.assertEqual(resources[1].label, "2")
        self.assertEqual(resources[2].label, "3")
        self.assertEqual(resources[3].label, "7")

    def make_dummy_resource_queue(self, num_resources: int) -> queue.Queue:
        resource_queue = queue.Queue()
        for i in range(num_resources):
            resource_queue.put(Resource(name="test-resource", label=i))
        return resource_queue

    def test_shell_task_initial_state(self) -> None:
        shell_task = ShellTask(f"echo 'Hello World' {RESOURCE_PLACEHOLDER}")
        self.assertEqual(shell_task.state, shell_task.state.WAITING)

    def test_shell_task_finished_state(self) -> None:
        resource_queue = self.make_dummy_resource_queue(1)
        shell_task = ShellTask(f"echo 'Hello World' {RESOURCE_PLACEHOLDER}")
        shell_task.run(resource_queue)
        self.assertEqual(shell_task.state, shell_task.state.FINISHED)

    def test_shell_task_dependency_single(self) -> None:
        resource_queue = self.make_dummy_resource_queue(1)
        dependency = ShellTask(f"echo 'Hello World' {RESOURCE_PLACEHOLDER}")
        task = ShellTask("echo 'Hello World'", depends_on=dependency)
        self.assertFalse(task.check_all_dependencies_finished())

        dependency.run(resource_queue)
        self.assertTrue(task.check_all_dependencies_finished())

    def test_shell_task_dependency_multiple(self) -> None:
        resource_queue = self.make_dummy_resource_queue(1)
        dependency_1 = ShellTask(f"echo 'Hello World' {RESOURCE_PLACEHOLDER}")
        dependency_2 = ShellTask(f"echo 'Hello World' {RESOURCE_PLACEHOLDER}")
        task = ShellTask(
            f"echo 'Hello World' {RESOURCE_PLACEHOLDER}",
            depends_on=[dependency_1, dependency_2],
        )
        self.assertFalse(task.check_all_dependencies_finished())

        dependency_1.run(resource_queue)
        dependency_2.run(resource_queue)
        self.assertTrue(task.check_all_dependencies_finished())

    def test_shell_task_dependency_wait(self) -> None:
        dependency = ShellTask(f"sleep 1 && echo 'Hello World' {RESOURCE_PLACEHOLDER}")
        task = ShellTask(
            f"echo 'Hello World' {RESOURCE_PLACEHOLDER}",
            depends_on=dependency,
        )

        # Measure time
        t_start = time.time()
        sleep_time = 2
        run_tasks([dependency], [Resource(name="test-resource", label=0)])
        task.wait_for_dependencies(sleep_time=sleep_time)

        # Check that the task waited for the dependency to finish
        t_end = time.time()
        self.assertGreater(t_end - t_start, sleep_time)

    def test_shell_task_dependency_complex(self) -> None:
        resource = Resource(name="test-resource", label=0)
        dependency_1 = ShellTask(f"sleep 1 && echo 'Hello World' {RESOURCE_PLACEHOLDER}")
        dependency_2 = ShellTask(f"sleep 1 && echo 'Hello World' {RESOURCE_PLACEHOLDER}")
        task = ShellTask(
            f"echo 'Hello World' {RESOURCE_PLACEHOLDER}",
            depends_on=[dependency_1, dependency_2],
        )

        # Measure time
        t_start = time.time()
        sleep_time = 2
        run_tasks([task, dependency_1, dependency_2, task], [resource])

        # Check that the task waited for the dependency to finish
        t_end = time.time()
        self.assertGreater(t_end - t_start, sleep_time)

        # Check that the task ran after the dependencies
        self.assertEqual(task.state, TaskState.FINISHED)
        self.assertEqual(dependency_1.state, TaskState.FINISHED)
        self.assertEqual(dependency_2.state, TaskState.FINISHED)


if __name__ == "__main__":
    unittest.main()  # pragma: no cover
