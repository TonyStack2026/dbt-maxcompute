import ast
import hashlib
import io
import keyword
import re
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from dbt_common.exceptions import DbtRuntimeError

SUPPORTED_PYTHON_UDF_TYPES = frozenset({"scalar", "aggregate"})
_PYTHON_RUNTIME_CONFIGS = {
    "3.11": ("cp311", (3, 11), "CPython 3.11"),
    "cp311": ("cp311", (3, 11), "CPython 3.11"),
    "python-3.11": ("cp311", (3, 11), "CPython 3.11"),
    "3.7": ("cp37", (3, 7), "CPython 3.7.3"),
    "3.7.3": ("cp37", (3, 7), "CPython 3.7.3"),
    "cp37": ("cp37", (3, 7), "CPython 3.7.3"),
    "python-3.7": ("cp37", (3, 7), "CPython 3.7.3"),
}
SUPPORTED_PYTHON_RUNTIMES = frozenset(_PYTHON_RUNTIME_CONFIGS)
PYTHON_UDF_HANDLER = "DbtMaxComputePythonFunction"
MANAGED_RESOURCE_COMMENT_PREFIX = "dbt-maxcompute managed Python UDF resource:"
_GENERATED_SYMBOLS = frozenset({"annotate", "BaseUDAF", PYTHON_UDF_HANDLER})

_SIMPLE_TYPE_ALIASES = {
    "bool": "boolean",
    "bytes": "binary",
    "float64": "double",
    "int64": "bigint",
    "integer": "bigint",
    "numeric": "decimal",
}


@dataclass(frozen=True)
class MaxComputePythonUDFSpec:
    function_name: str
    project: str
    schema: str
    compiled_code: str
    argument_types: Tuple[str, ...]
    return_type: str
    entry_point: str
    runtime_version: str
    function_type: str = "scalar"
    packages: Tuple[str, ...] = ()
    resources: Tuple[str, ...] = ()
    python_libraries: Tuple[str, ...] = ()


@dataclass(frozen=True)
class MaxComputePythonUDFDeployment:
    source: str
    signature: str
    resource_name: str
    class_type: str
    resource_comment: str
    runtime_version: str


def _runtime_error(message: str) -> DbtRuntimeError:
    return DbtRuntimeError(f"MaxCompute Python UDF: {message}")


def _parse_python(source: str, feature_version: Tuple[int, int], runtime_name: str) -> ast.Module:
    try:
        try:
            return ast.parse(
                source,
                filename="<dbt MaxCompute Python UDF>",
                feature_version=feature_version,
            )
        except TypeError:
            # Python 3.10 accepts the minor version as an integer while newer
            # versions also accept a (major, minor) tuple.
            return ast.parse(
                source,
                filename="<dbt MaxCompute Python UDF>",
                feature_version=feature_version[1],
            )
    except SyntaxError as exc:
        location = f"line {exc.lineno}" if exc.lineno else "an unknown line"
        raise _runtime_error(
            f"the function source is not valid for the MaxCompute {runtime_name} "
            f"runtime ({location}: {exc.msg})"
        ) from exc


def _normalize_runtime_version(
    runtime_version: str,
) -> Tuple[str, Tuple[int, int], str]:
    normalized = str(runtime_version or "").strip().lower()
    runtime_config = _PYTHON_RUNTIME_CONFIGS.get(normalized)
    if runtime_config is None:
        supported = ", ".join(sorted(SUPPORTED_PYTHON_RUNTIMES))
        raise _runtime_error(
            f"runtime_version={runtime_version!r} is unsupported. " f"Use one of: {supported}"
        )
    return runtime_config


def _normalize_function_type(function_type: str) -> str:
    normalized = str(function_type or "scalar").strip().lower()
    if normalized not in SUPPORTED_PYTHON_UDF_TYPES:
        supported = ", ".join(sorted(SUPPORTED_PYTHON_UDF_TYPES))
        raise _runtime_error(
            f"type={function_type!r} is unsupported; supported Python function "
            f"types are: {supported}"
        )
    return normalized


def _normalize_data_type(data_type: str) -> str:
    normalized = str(data_type or "").strip()
    if not normalized:
        raise _runtime_error("function arguments and returns must declare a data_type")

    def replace_type_token(match: re.Match) -> str:
        token = match.group(0).lower()
        # A token followed by ':' is a STRUCT field name, not a data type.
        if normalized[match.end() :].lstrip().startswith(":"):
            return token
        return _SIMPLE_TYPE_ALIASES.get(token, token)

    return re.sub(r"[A-Za-z_][A-Za-z0-9_]*", replace_type_token, normalized)


def _find_entry_point(module: ast.Module, entry_point: str) -> ast.AST:
    definitions = [
        node
        for node in module.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef))
        and node.name == entry_point
    ]
    if not definitions:
        raise _runtime_error(
            f"entry_point={entry_point!r} was not found as a top-level function or class"
        )
    if len(definitions) > 1:
        raise _runtime_error(f"entry_point={entry_point!r} is defined more than once")
    return definitions[0]


def _validate_python_identifier(value: str, field_name: str) -> None:
    if not value or not value.isidentifier() or keyword.iskeyword(value):
        raise _runtime_error(f"{field_name}={value!r} is not a valid Python identifier")


def _validate_callable_arity(
    node: ast.FunctionDef, argument_count: int, *, skip_first: bool = False
) -> None:
    positional = list(node.args.posonlyargs) + list(node.args.args)
    if skip_first:
        if not positional:
            raise _runtime_error(f"method {node.name!r} must declare self")
        positional = positional[1:]

    defaults = list(node.args.defaults)
    required_positional = max(0, len(positional) - len(defaults))
    max_positional: Optional[int] = None if node.args.vararg else len(positional)
    required_kwonly = [
        arg for arg, default in zip(node.args.kwonlyargs, node.args.kw_defaults) if default is None
    ]
    if required_kwonly:
        names = ", ".join(arg.arg for arg in required_kwonly)
        raise _runtime_error(
            f"entry point {node.name!r} has required keyword-only arguments ({names}); "
            "MaxCompute invokes UDF arguments positionally"
        )
    if argument_count < required_positional or (
        max_positional is not None and argument_count > max_positional
    ):
        maximum = "unbounded" if max_positional is None else str(max_positional)
        raise _runtime_error(
            f"entry point {node.name!r} accepts {required_positional}..{maximum} "
            f"positional arguments, but YAML declares {argument_count}"
        )


def _class_method(node: ast.ClassDef, method_name: str) -> Optional[ast.FunctionDef]:
    for child in node.body:
        if isinstance(child, ast.FunctionDef) and child.name == method_name:
            return child
    return None


def _validate_entry_point(
    entry_point_node: ast.AST, function_type: str, argument_count: int
) -> str:
    if function_type == "scalar":
        if isinstance(entry_point_node, ast.AsyncFunctionDef):
            raise _runtime_error("async scalar entry points are not supported")
        if isinstance(entry_point_node, ast.FunctionDef):
            _validate_callable_arity(entry_point_node, argument_count)
            return "function"
        if isinstance(entry_point_node, ast.ClassDef):
            initializer = _class_method(entry_point_node, "__init__")
            if initializer is not None:
                _validate_callable_arity(initializer, 0, skip_first=True)
            evaluate = _class_method(entry_point_node, "evaluate")
            if evaluate is None:
                raise _runtime_error(
                    f"scalar class {entry_point_node.name!r} must define evaluate()"
                )
            _validate_callable_arity(evaluate, argument_count, skip_first=True)
            return "class"

    if function_type == "aggregate":
        if not isinstance(entry_point_node, ast.ClassDef):
            raise _runtime_error(
                "aggregate entry_point must be a class implementing the MaxCompute "
                "BaseUDAF lifecycle"
            )
        required_methods = ("new_buffer", "iterate", "merge", "terminate")
        methods = {method: _class_method(entry_point_node, method) for method in required_methods}
        missing = sorted(
            method for method, implementation in methods.items() if implementation is None
        )
        if missing:
            raise _runtime_error(
                f"aggregate class {entry_point_node.name!r} is missing required "
                f"BaseUDAF methods: {', '.join(missing)}"
            )
        initializer = _class_method(entry_point_node, "__init__")
        if initializer is not None:
            _validate_callable_arity(initializer, 0, skip_first=True)
        new_buffer = methods["new_buffer"]
        iterate = methods["iterate"]
        merge = methods["merge"]
        terminate = methods["terminate"]
        assert new_buffer is not None
        assert iterate is not None
        assert merge is not None
        assert terminate is not None
        _validate_callable_arity(new_buffer, 0, skip_first=True)
        _validate_callable_arity(
            iterate,
            argument_count + 1,
            skip_first=True,
        )
        _validate_callable_arity(merge, 2, skip_first=True)
        _validate_callable_arity(terminate, 1, skip_first=True)
        return "class"

    raise _runtime_error(f"unable to validate function type {function_type!r}")


def _library_setup_source(python_libraries: Sequence[str]) -> str:
    if not python_libraries:
        return ""
    lines = [
        "# Add declared MaxCompute archive resources before user imports.",
        "import sys as _dbt_sys",
    ]
    for resource_name in python_libraries:
        lines.append(f"_dbt_sys.path.insert(0, {'work/' + resource_name!r})")
    return "\n".join(lines) + "\n"


def _inject_library_setup(source: str, module: ast.Module, python_libraries: Sequence[str]) -> str:
    setup_source = _library_setup_source(python_libraries)
    if not setup_source:
        return source

    insertion_line = 0
    body = list(module.body)
    if body and isinstance(body[0], ast.Expr):
        value = body[0].value
        if isinstance(value, ast.Constant) and isinstance(value.value, str):
            insertion_line = body.pop(0).end_lineno or 0
    for node in body:
        if isinstance(node, ast.ImportFrom) and node.module == "__future__":
            insertion_line = node.end_lineno or insertion_line
        else:
            break

    lines = source.splitlines(keepends=True)
    if lines and not lines[-1].endswith(("\n", "\r")):
        lines[-1] += "\n"
    lines.insert(insertion_line, setup_source)
    return "".join(lines)


def _scalar_wrapper(
    entry_point: str,
    argument_count: int,
    entry_point_kind: str,
    signature: str,
) -> str:
    if entry_point_kind == "class":
        return "\n".join(
            [
                f"@annotate({signature!r})",
                f"class {PYTHON_UDF_HANDLER}({entry_point}):",
                "    pass",
            ]
        )

    arguments = [f"_dbt_arg_{idx}" for idx in range(argument_count)]
    declaration = ", ".join(["self"] + arguments)
    invocation = ", ".join(arguments)
    return "\n".join(
        [
            f"@annotate({signature!r})",
            f"class {PYTHON_UDF_HANDLER}(object):",
            f"    def evaluate({declaration}):",
            f"        return {entry_point}({invocation})",
        ]
    )


def _aggregate_wrapper(entry_point: str, signature: str) -> str:
    return "\n".join(
        [
            f"@annotate({signature!r})",
            f"class {PYTHON_UDF_HANDLER}(BaseUDAF):",
            "    def __init__(self):",
            f"        self._dbt_implementation = {entry_point}()",
            "",
            "    def new_buffer(self):",
            "        return self._dbt_implementation.new_buffer()",
            "",
            "    def iterate(self, buffer, *args):",
            "        return self._dbt_implementation.iterate(buffer, *args)",
            "",
            "    def merge(self, buffer, partial):",
            "        return self._dbt_implementation.merge(buffer, partial)",
            "",
            "    def terminate(self, buffer):",
            "        return self._dbt_implementation.terminate(buffer)",
        ]
    )


def _resource_slug(function_name: str) -> str:
    slug = re.sub(r"[^a-zA-Z0-9_]+", "_", function_name).strip("_").lower()
    return (slug or "function")[:64]


def managed_resource_comment(spec: MaxComputePythonUDFSpec) -> str:
    return (
        f"{MANAGED_RESOURCE_COMMENT_PREFIX}" f"{spec.project}.{spec.schema}.{spec.function_name}"
    )


def build_python_udf_deployment(
    spec: MaxComputePythonUDFSpec,
) -> MaxComputePythonUDFDeployment:
    _validate_python_identifier(spec.entry_point, "entry_point")
    if spec.entry_point in _GENERATED_SYMBOLS:
        raise _runtime_error(
            f"entry_point={spec.entry_point!r} conflicts with a generated MaxCompute "
            "UDF symbol; rename the user entry point"
        )
    runtime_version, feature_version, runtime_name = _normalize_runtime_version(
        spec.runtime_version
    )
    function_type = _normalize_function_type(spec.function_type)
    if spec.packages:
        raise _runtime_error(
            "the dbt packages config cannot be installed dynamically by MaxCompute. "
            f"Upload compatible {runtime_name} archives first and list them under "
            "config.maxcompute.python_libraries"
        )

    module = _parse_python(spec.compiled_code, feature_version, runtime_name)
    entry_point_node = _find_entry_point(module, spec.entry_point)
    entry_point_kind = _validate_entry_point(
        entry_point_node, function_type, len(spec.argument_types)
    )

    argument_types = [_normalize_data_type(value) for value in spec.argument_types]
    return_type = _normalize_data_type(spec.return_type)
    signature = f"{','.join(argument_types)}->{return_type}"

    source = _inject_library_setup(spec.compiled_code, module, spec.python_libraries)
    source = source.rstrip() + "\n\n"
    source += "from odps.udf import annotate\n"
    if function_type == "aggregate":
        source += "from odps.udf import BaseUDAF\n"
    source += "\n"
    if function_type == "scalar":
        source += _scalar_wrapper(
            spec.entry_point,
            len(spec.argument_types),
            entry_point_kind,
            signature,
        )
    else:
        source += _aggregate_wrapper(spec.entry_point, signature)
    source += "\n"

    # Validate the complete generated module against the declared runtime.
    _parse_python(source, feature_version, runtime_name)
    digest = hashlib.sha256(source.encode("utf-8")).hexdigest()[:16]
    resource_name = f"dbt_udf_{_resource_slug(spec.function_name)}_{digest}.py"
    module_name = resource_name[:-3]
    return MaxComputePythonUDFDeployment(
        source=source,
        signature=signature,
        resource_name=resource_name,
        class_type=f"{module_name}.{PYTHON_UDF_HANDLER}",
        resource_comment=managed_resource_comment(spec),
        runtime_version=runtime_version,
    )


def _deduplicate(values: Iterable[str]) -> List[str]:
    result: List[str] = []
    seen = set()
    for raw_value in values:
        value = str(raw_value or "").strip()
        if not value:
            raise _runtime_error("resource names cannot be empty")
        if value not in seen:
            seen.add(value)
            result.append(value)
    return result


class MaxComputePythonUDFManager:
    def __init__(self, odps_client: Any, logger: Optional[Any] = None):
        self.odps = odps_client
        self.logger = logger

    def _log_warning(self, message: str) -> None:
        if self.logger is not None:
            self.logger.warning(message)

    def _get_resource(self, name: str, spec: MaxComputePythonUDFSpec) -> Any:
        if not self.odps.exist_resource(name, project=spec.project, schema=spec.schema):
            raise _runtime_error(
                f"declared resource {name!r} does not exist in " f"{spec.project}.{spec.schema}"
            )
        resource = self.odps.get_resource(name, project=spec.project, schema=spec.schema)
        resource.reload()
        return resource

    def _ensure_code_resource(
        self,
        deployment: MaxComputePythonUDFDeployment,
        spec: MaxComputePythonUDFSpec,
    ) -> Tuple[Any, bool]:
        if self.odps.exist_resource(
            deployment.resource_name, project=spec.project, schema=spec.schema
        ):
            resource = self.odps.get_resource(
                deployment.resource_name, project=spec.project, schema=spec.schema
            )
            resource.reload()
            if resource.comment != deployment.resource_comment:
                raise _runtime_error(
                    f"resource {deployment.resource_name!r} already exists but is not "
                    "owned by this dbt function; refusing to overwrite it"
                )
            return resource, False

        resource = self.odps.create_resource(
            deployment.resource_name,
            "py",
            project=spec.project,
            schema=spec.schema,
            fileobj=io.BytesIO(deployment.source.encode("utf-8")),
            comment=deployment.resource_comment,
        )
        return resource, True

    def _function_uses_resource(self, spec: MaxComputePythonUDFSpec, resource_name: str) -> bool:
        try:
            if not self.odps.exist_function(
                spec.function_name, project=spec.project, schema=spec.schema
            ):
                return False
            function = self.odps.get_function(
                spec.function_name, project=spec.project, schema=spec.schema
            )
            function.reload()
            return resource_name in {resource.name for resource in function.resources}
        except Exception as exc:
            self._log_warning(
                f"Could not verify resource usage after a Python UDF deployment " f"failure: {exc}"
            )
            # Unknown is treated as in use so cleanup cannot break a function
            # whose update succeeded but whose response was interrupted.
            return True

    def _delete_resource(self, resource_name: str, spec: MaxComputePythonUDFSpec) -> None:
        self.odps.delete_resource(resource_name, project=spec.project, schema=spec.schema)

    def _cleanup_managed_resources(
        self,
        resources: Sequence[Any],
        spec: MaxComputePythonUDFSpec,
        keep_names: Iterable[str] = (),
    ) -> None:
        keep = set(keep_names)
        visited = set()
        expected_comment = managed_resource_comment(spec)
        for resource in resources:
            if resource.name in keep or resource.name in visited:
                continue
            visited.add(resource.name)
            try:
                resource.reload()
                if resource.comment != expected_comment:
                    continue
                self._delete_resource(resource.name, spec)
            except Exception as exc:
                self._log_warning(
                    f"Could not clean obsolete dbt-managed Python UDF resource "
                    f"{resource.name!r}: {exc}"
                )

    def _list_managed_code_resources(self, spec: MaxComputePythonUDFSpec) -> Sequence[Any]:
        prefix = f"dbt_udf_{_resource_slug(spec.function_name)}_"
        try:
            return list(
                self.odps.list_resources(
                    project=spec.project,
                    schema=spec.schema,
                    prefix=prefix,
                )
            )
        except Exception as exc:
            self._log_warning(f"Could not list stale dbt-managed Python UDF resources: {exc}")
            return ()

    def deploy(self, spec: MaxComputePythonUDFSpec) -> Dict[str, str]:
        deployment = build_python_udf_deployment(spec)
        dependency_names = _deduplicate(list(spec.resources) + list(spec.python_libraries))
        dependencies = [self._get_resource(name, spec) for name in dependency_names]
        code_resource, code_resource_created = self._ensure_code_resource(deployment, spec)
        function_resources = [code_resource] + dependencies
        old_resources: Sequence[Any] = ()
        action = "created"

        try:
            if self.odps.exist_function(
                spec.function_name, project=spec.project, schema=spec.schema
            ):
                function = self.odps.get_function(
                    spec.function_name, project=spec.project, schema=spec.schema
                )
                function.reload()
                old_resources = list(function.resources)
                function.class_type = deployment.class_type
                function.resources = function_resources
                function.update()
                action = "updated"
            else:
                self.odps.create_function(
                    spec.function_name,
                    project=spec.project,
                    schema=spec.schema,
                    class_type=deployment.class_type,
                    resources=function_resources,
                )
        except Exception:
            if code_resource_created and not self._function_uses_resource(
                spec, deployment.resource_name
            ):
                try:
                    self._delete_resource(deployment.resource_name, spec)
                except Exception as cleanup_exc:
                    self._log_warning(
                        f"Could not clean Python UDF resource after deployment "
                        f"failure: {cleanup_exc}"
                    )
            raise

        cleanup_candidates = list(old_resources) + list(self._list_managed_code_resources(spec))
        self._cleanup_managed_resources(
            cleanup_candidates,
            spec,
            keep_names=[resource.name for resource in function_resources],
        )
        return {
            "action": action,
            "function_name": spec.function_name,
            "resource_name": deployment.resource_name,
            "signature": deployment.signature,
            "runtime_version": deployment.runtime_version,
        }

    def drop(self, function_name: str, project: str, schema: str) -> None:
        spec = MaxComputePythonUDFSpec(
            function_name=function_name,
            project=project,
            schema=schema,
            compiled_code="",
            argument_types=(),
            return_type="string",
            entry_point="unused",
            runtime_version="3.11",
        )
        resources: List[Any] = []
        if self.odps.exist_function(function_name, project=project, schema=schema):
            function = self.odps.get_function(function_name, project=project, schema=schema)
            function.reload()
            resources = list(function.resources)
            self.odps.delete_function(function_name, project=project, schema=schema)
        cleanup_candidates = resources + list(self._list_managed_code_resources(spec))
        self._cleanup_managed_resources(cleanup_candidates, spec)
