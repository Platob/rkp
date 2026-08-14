from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import tomllib


def test_json_yaml_and_arrow_work_without_optional_dependencies() -> None:
    script = r"""
import builtins
real_import = builtins.__import__
def guarded(name, *args, **kwargs):
    if name.split(".", 1)[0] in {"boto3", "botocore", "pyiceberg", "yaml"}:
        raise ModuleNotFoundError(f"blocked {name}", name=name.split(".", 1)[0])
    return real_import(name, *args, **kwargs)
builtins.__import__ = guarded
from rkp import Record, record
@record
class Value(Record):
    number: int
assert Value.loads_json(Value(3).dumps_json()) == Value(3)
assert Value.loads_yaml(Value(3).dumps_yaml()) == Value(3)
assert Value.into_arrow_schema().names == ["number"]
"""
    environment = dict(os.environ)
    source = str(Path(__file__).resolve().parents[1] / "src")
    environment["PYTHONPATH"] = source
    completed = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        env=environment,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr


def test_pyarrow_is_required_and_pyiceberg_remains_optional() -> None:
    project_root = Path(__file__).resolve().parents[1]
    config = tomllib.loads(
        (project_root / "pyproject.toml").read_text(encoding="utf-8")
    )["project"]

    assert config["dependencies"] == [
        "pyarrow>=16.0.0",
        "tzdata>=2025.2; sys_platform == 'win32'",
    ]
    extras = config["optional-dependencies"]
    assert "arrow" not in extras
    assert extras["iceberg"] == ["pyiceberg[pyarrow]>=0.11.1,<0.12"]
    assert extras["spark"] == ["pyarrow>=18.0.0", "pyspark>=4.0,<5"]
    assert all(
        not requirement.startswith("pyarrow")
        for name, requirements in extras.items()
        if name not in {"spark", "all", "test"}
        for requirement in requirements
    )
    assert all(
        not requirement.startswith("pyiceberg")
        for requirement in config["dependencies"]
    )


def test_awsglue_has_an_optional_runtime_and_moto_test_extra() -> None:
    project_root = Path(__file__).resolve().parents[1]
    config = tomllib.loads(
        (project_root / "pyproject.toml").read_text(encoding="utf-8")
    )["project"]
    extras = config["optional-dependencies"]

    assert any(requirement.startswith("boto3") for requirement in extras["awsglue"])
    assert any(requirement.startswith("boto3") for requirement in extras["all"])
    assert any(requirement.startswith("moto") for requirement in extras["test"])
    assert all(
        not requirement.startswith(("boto3", "botocore", "moto"))
        for requirement in config["dependencies"]
    )


def test_glue_schema_and_ddl_work_without_the_aws_sdk() -> None:
    script = r"""
import builtins
real_import = builtins.__import__
def guarded(name, *args, **kwargs):
    if name.split(".", 1)[0] in {"boto3", "botocore"}:
        raise ModuleNotFoundError(f"blocked {name}", name=name.split(".", 1)[0])
    return real_import(name, *args, **kwargs)
builtins.__import__ = guarded
from rkp import GlueCatalog, Record, field, into_glue_ddl, into_glue_table_input, record
@record
class Event(Record):
    identifier: int
    day: str = field(partition_key=True)
table = into_glue_table_input(Event, location="s3://warehouse/events/")
assert table["Name"] == "event"
assert table["PartitionKeys"][0]["Name"] == "day"
assert "CREATE EXTERNAL TABLE" in into_glue_ddl(
    Event,
    location="s3://warehouse/events/",
)
try:
    GlueCatalog()
except ImportError as exc:
    assert "boto3" in str(exc).lower()
else:
    raise AssertionError("GlueCatalog() accepted a missing boto3 dependency")
"""
    environment = dict(os.environ)
    source = str(Path(__file__).resolve().parents[1] / "src")
    environment["PYTHONPATH"] = source
    completed = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        env=environment,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr


def test_spark_adapter_is_lazy_and_reports_the_extra() -> None:
    script = r"""
import builtins
real_import = builtins.__import__
def guarded(name, *args, **kwargs):
    if name.split(".", 1)[0] == "pyspark":
        raise ModuleNotFoundError(f"blocked {name}", name="pyspark")
    return real_import(name, *args, **kwargs)
builtins.__import__ = guarded
from rkp import Record, into_spark_schema, record
@record
class Event(Record):
    identifier: int
assert Event.into_arrow_schema().names == ["identifier"]
for operation in (lambda: into_spark_schema(Event), Event.into_spark_schema):
    try:
        operation()
    except ImportError as exc:
        assert "rkp[spark]" in str(exc)
    else:
        raise AssertionError("Spark conversion accepted a missing PySpark dependency")
"""
    environment = dict(os.environ)
    source = str(Path(__file__).resolve().parents[1] / "src")
    environment["PYTHONPATH"] = source
    completed = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        env=environment,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr
