# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import annotations

from airflow.providers.cncf.kubernetes.python_kubernetes_script import (
    remove_task_decorator,
    write_python_script,
)


def test_remove_task_decorator_removes_plain_decorators():
    source = (
        "@setup\n"
        "@task.kubernetes\n"
        "@teardown\n"
        "def run_task():\n"
        "    return 1\n"
    )

    assert remove_task_decorator(source, "@task.kubernetes") == "def run_task():\n    return 1\n"


def test_remove_task_decorator_removes_nested_arguments():
    source = (
        "@task.kubernetes(\n"
        "    image=\"python:3.12\",\n"
        "    command=(\"python\", (\"-c\", \"print(1)\")),\n"
        ")\n"
        "def run_task():\n"
        "    return 1\n"
    )

    assert remove_task_decorator(source, "@task.kubernetes") == "def run_task():\n    return 1\n"


def test_remove_task_decorator_keeps_source_without_matching_decorators():
    source = "def run_task():\n    return 1\n"

    assert remove_task_decorator(source, "@task.kubernetes") == source


def test_write_python_script_writes_script_without_arguments(tmp_path):
    filename = tmp_path / "script.py"

    write_python_script(
        jinja_context={
            "op_args": [],
            "op_kwargs": {},
            "pickling_library": "pickle",
            "python_callable": "run_task",
            "python_callable_source": "def run_task():\n    return 1",
        },
        filename=str(filename),
    )

    script = filename.read_text()
    assert 'arg_dict = {"args": [], "kwargs": {}}' in script
    assert "res = run_task(*arg_dict[\"args\"], **arg_dict[\"kwargs\"])" in script


def test_write_python_script_loads_arguments_when_present(tmp_path):
    filename = tmp_path / "script.py"

    write_python_script(
        jinja_context={
            "op_args": ["value"],
            "op_kwargs": {},
            "pickling_library": "pickle",
            "python_callable": "run_task",
            "python_callable_source": "def run_task(value):\n    return value",
        },
        filename=str(filename),
    )

    script = filename.read_text()
    assert 'with open(sys.argv[1], "rb") as file:' in script
    assert "arg_dict = pickle.load(file)" in script
