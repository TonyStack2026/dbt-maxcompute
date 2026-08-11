#!/usr/bin/env python3
"""Build, verify, and optionally upload the showcase CPython 3.11 UDF archive."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import tempfile
import zipfile
from pathlib import Path
from typing import Iterable


RESOURCE_NAME = "dbt_showcase_pydeps_cp311.zip"
IMPORT_CHECK = (
    "import dateutil, jmespath, phonenumbers, text_unidecode; "
    "print('dependency archive import check: OK')"
)
NATIVE_SUFFIXES = (".so", ".dylib", ".dll", ".pyd")


def parse_args() -> argparse.Namespace:
    project_dir = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(
        description=(
            "Build a pure-Python dependency archive for the showcase catalog "
            "UDFs and optionally upload it as a MaxCompute ARCHIVE resource."
        )
    )
    parser.add_argument(
        "--requirements",
        type=Path,
        default=project_dir / "requirements-udf-cp311.txt",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=project_dir / "target" / RESOURCE_NAME,
    )
    parser.add_argument("--upload", action="store_true")
    parser.add_argument("--replace", action="store_true")
    parser.add_argument("--project")
    parser.add_argument("--schema")
    parser.add_argument("--endpoint")
    return parser.parse_args()


def iter_files(root: Path) -> Iterable[Path]:
    for path in sorted(root.rglob("*")):
        if not path.is_file():
            continue
        if "__pycache__" in path.parts or path.suffix in {".pyc", ".pyo"}:
            continue
        yield path


def read_requirements(path: Path) -> list[str]:
    requirements = []
    for line in path.read_text(encoding="utf-8").splitlines():
        value = line.partition("#")[0].strip()
        if value:
            requirements.append(value)
    if not requirements:
        raise SystemExit(f"No requirements found in {path}")
    return requirements


def build_archive(requirements_path: Path, output: Path) -> None:
    requirements = read_requirements(requirements_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="dbt-maxcompute-udf-deps-") as temp_dir:
        package_root = Path(temp_dir) / "site-packages"
        package_root.mkdir()
        command = [
            sys.executable,
            "-m",
            "pip",
            "install",
            "--disable-pip-version-check",
            "--only-binary=:all:",
            "--no-deps",
            "--no-compile",
            "--requirement",
            str(requirements_path),
            "--target",
            str(package_root),
        ]
        subprocess.run(command, check=True)

        native_files = [
            path.relative_to(package_root).as_posix()
            for path in iter_files(package_root)
            if path.name.endswith(NATIVE_SUFFIXES)
        ]
        if native_files:
            formatted = "\n  - ".join(native_files)
            raise SystemExit(
                "The catalog UDF archive must remain pure Python; native "
                f"artifacts were found:\n  - {formatted}"
            )

        manifest = {
            "python_runtime": "cp311",
            "requirements": requirements,
            "resource_name": RESOURCE_NAME,
        }
        (package_root / "dbt_showcase_dependency_manifest.json").write_text(
            json.dumps(manifest, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        temporary_output = output.with_suffix(output.suffix + ".tmp")
        if temporary_output.exists():
            temporary_output.unlink()
        with zipfile.ZipFile(
            temporary_output,
            mode="w",
            compression=zipfile.ZIP_DEFLATED,
            compresslevel=9,
        ) as archive:
            for path in iter_files(package_root):
                archive.write(path, path.relative_to(package_root).as_posix())
        temporary_output.replace(output)


def verify_archive(output: Path) -> None:
    environment = os.environ.copy()
    existing_pythonpath = environment.get("PYTHONPATH")
    environment["PYTHONPATH"] = (
        str(output)
        if not existing_pythonpath
        else os.pathsep.join([str(output), existing_pythonpath])
    )
    subprocess.run([sys.executable, "-c", IMPORT_CHECK], check=True, env=environment)
    with zipfile.ZipFile(output) as archive:
        bad_entry = archive.testzip()
        if bad_entry:
            raise SystemExit(f"Corrupt zip entry: {bad_entry}")


def required_argument(value: str | None, flag: str) -> str:
    if value:
        return value
    raise SystemExit(f"{flag} is required with --upload")


def upload_archive(args: argparse.Namespace) -> None:
    from odps import ODPS

    project = required_argument(args.project, "--project")
    schema = required_argument(args.schema, "--schema")
    endpoint = required_argument(args.endpoint, "--endpoint")
    access_id = os.getenv("ODPS_ACCESS_ID") or os.getenv("ALIBABA_CLOUD_ACCESS_KEY_ID")
    secret = os.getenv("ODPS_SECRET_ACCESS_KEY") or os.getenv("ALIBABA_CLOUD_ACCESS_KEY_SECRET")
    if not access_id or not secret:
        raise SystemExit(
            "Set ODPS_ACCESS_ID / ODPS_SECRET_ACCESS_KEY (or the corresponding "
            "ALIBABA_CLOUD_* variables) before using --upload."
        )

    client = ODPS(access_id, secret, project=project, endpoint=endpoint)
    if client.exist_resource(RESOURCE_NAME, project=project, schema=schema):
        if not args.replace:
            raise SystemExit(
                f"Resource {project}.{schema}.{RESOURCE_NAME} already exists. "
                "Rerun with --replace only if this exact showcase resource may be replaced."
            )
        client.delete_resource(RESOURCE_NAME, project=project, schema=schema)

    with args.output.open("rb") as fileobj:
        client.create_resource(
            RESOURCE_NAME,
            "archive",
            project=project,
            schema=schema,
            fileobj=fileobj,
        )
    print(f"uploaded archive resource: {project}.{schema}.{RESOURCE_NAME}")


def main() -> None:
    args = parse_args()
    build_archive(args.requirements.resolve(), args.output.resolve())
    verify_archive(args.output.resolve())
    print(f"built dependency archive: {args.output.resolve()}")
    if args.upload:
        upload_archive(args)


if __name__ == "__main__":
    main()
