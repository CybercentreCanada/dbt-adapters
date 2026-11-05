import ast
import hashlib
import json
import keyword
import sys
import threading
from types import CodeType
from dataclasses import asdict, dataclass
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple, TYPE_CHECKING

from dbt.adapters.spark.relation_configs.write_order import quote_identifier
from dbt_common.exceptions import DbtRuntimeError

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


def _identifier(value: Any, label: str, python: bool = False) -> str:
    if not isinstance(value, str) or not value or "\x00" in value:
        raise DbtRuntimeError(f"Invalid {label} for Spark function: {value!r}")
    if python and (not value.isidentifier() or keyword.iskeyword(value)):
        raise DbtRuntimeError(f"Spark Python function {label} must be a Python identifier")
    return value


@dataclass(frozen=True)
class SparkFunction:
    unique_id: str
    identifier: str
    language: str
    arguments: Tuple[Tuple[str, str], ...]
    return_type: str
    body: str
    dependencies: Tuple[str, ...]
    entry_point: Optional[str] = None
    runtime_version: Optional[str] = None
    volatility: Optional[str] = None

    @classmethod
    def from_model(cls, model: Mapping[str, Any]) -> "SparkFunction":
        unique_id = _identifier(model.get("unique_id"), "unique_id")
        config = model.get("config", {})
        if not isinstance(config, Mapping):
            raise DbtRuntimeError(f"Invalid configuration for Spark function {unique_id}")
        if config.get("spark_function_scope") != "session":
            raise DbtRuntimeError(
                f"Spark function {unique_id} requires spark_function_scope: session; "
                "persistent UDF deployment to Polaris is not supported"
            )
        language = model.get("language")
        if config.get("type", "scalar") != "scalar" or language not in ("sql", "python"):
            raise DbtRuntimeError(f"Spark function {unique_id} supports only SQL/Python scalars")
        if model.get("overloads"):
            raise DbtRuntimeError(f"Spark function {unique_id} does not support overloads")
        docs = config.get("persist_docs") or {}
        if not isinstance(docs, Mapping):
            raise DbtRuntimeError(f"Invalid persist_docs for Spark function {unique_id}")
        if config.get("grants") or any(docs.values()):
            raise DbtRuntimeError(
                f"Spark function {unique_id} cannot persist grants or documentation"
            )
        volatility = config.get("volatility")
        if volatility not in (None, "deterministic", "non-deterministic"):
            raise DbtRuntimeError(f"Unsupported volatility for Spark function {unique_id}")
        arguments = []
        for argument in model.get("arguments", []):
            if not isinstance(argument, Mapping):
                raise DbtRuntimeError(f"Invalid argument for Spark function {unique_id}")
            if argument.get("default_value") is not None:
                raise DbtRuntimeError(f"Spark function {unique_id} does not support defaults")
            arguments.append(
                (
                    _identifier(argument.get("name"), "argument", language == "python"),
                    _identifier(argument.get("data_type"), "argument data_type"),
                )
            )
        names = [name for name, _ in arguments]
        if len(names) != len(set(names)):
            raise DbtRuntimeError(f"Duplicate arguments for Spark function {unique_id}")
        body = model.get("compiled_code")
        if not isinstance(body, str) or not body.strip():
            raise DbtRuntimeError(f"Spark function {unique_id} requires a compiled body")
        entry_point = config.get("entry_point")
        runtime_version = config.get("runtime_version")
        if language == "python":
            entry_point = _identifier(entry_point, "entry_point", python=True)
            expected_runtime = f"{sys.version_info.major}.{sys.version_info.minor}"
            if runtime_version != expected_runtime:
                raise DbtRuntimeError(
                    f"Spark function {unique_id} runtime_version must match the driver "
                    f"({expected_runtime}); no interpreter is provisioned"
                )
        returns = model.get("returns") or {}
        if not isinstance(returns, Mapping):
            raise DbtRuntimeError(f"Invalid returns for Spark function {unique_id}")
        function = cls(
            unique_id=unique_id,
            identifier=_identifier(
                model.get("identifier")
                or model.get("alias")
                or config.get("alias")
                or model.get("name"),
                "name",
            ),
            language=language,
            arguments=tuple(arguments),
            return_type=_identifier(returns.get("data_type"), "return type"),
            body=body,
            dependencies=tuple(
                dep
                for dep in model.get("depends_on", {}).get("nodes", [])
                if dep.startswith("function.")
            ),
            entry_point=entry_point,
            runtime_version=runtime_version,
            volatility=volatility,
        )
        if language == "python":
            function.python_code()
        return function

    @property
    def fingerprint(self) -> str:
        return hashlib.sha256(json.dumps(asdict(self), sort_keys=True).encode()).hexdigest()

    def sql(self) -> str:
        args = ", ".join(f"{quote_identifier(name)} {dtype}" for name, dtype in self.arguments)
        volatility = {
            None: "",
            "deterministic": " DETERMINISTIC",
            "non-deterministic": " NOT DETERMINISTIC",
        }[self.volatility]
        return (
            f"CREATE OR REPLACE TEMPORARY FUNCTION {quote_identifier(self.identifier)}({args}) "
            f"RETURNS {self.return_type} LANGUAGE SQL{volatility} "
            f"RETURN {self.body.strip().removesuffix(';')}"
        )

    def python_code(self) -> CodeType:
        try:
            body = ast.parse(self.body, filename=self.unique_id).body
            wrapper = ast.parse("def implementation():\n    pass").body[0]
            if not isinstance(wrapper, ast.FunctionDef):
                raise DbtRuntimeError("Cannot construct Spark Python function wrapper")
            wrapper.name = self.entry_point or ""
            wrapper.args.args = [ast.arg(arg=name) for name, _ in self.arguments]
            wrapper.body = body
            module = ast.fix_missing_locations(ast.Module(body=[wrapper], type_ignores=[]))
            return compile(module, self.unique_id, "exec")
        except (SyntaxError, ValueError, TypeError) as exc:
            raise DbtRuntimeError(
                f"Invalid Python body for Spark function {self.unique_id}"
            ) from exc

    def validate_session(self, spark: "SparkSession") -> None:
        version = tuple(int(part) for part in spark.version.split(".")[:2])
        if version < (4, 0) or version >= (5, 0):
            raise DbtRuntimeError(f"Spark function {self.unique_id} requires Apache Spark 4.x")
        extensions = spark.conf.get("spark.sql.extensions", "")
        if self.language == "sql" and "IcebergSparkSessionExtensions" in extensions:
            raise DbtRuntimeError(
                f"SQL function {self.unique_id} requires the native Spark parser: "
                "IcebergSparkSessionExtensions in the tested Iceberg 1.12.0 runtime "
                "does not implement parseRoutineParam. Omit that extension and explicitly "
                "enable flags.spark_table_only for Polaris table discovery."
            )
        parser = spark._jsparkSession.sessionState().sqlParser()
        try:
            for dtype in [self.return_type, *(dtype for _, dtype in self.arguments)]:
                parser.parseDataType(dtype)
        except Exception as exc:
            raise DbtRuntimeError(
                f"Invalid Spark data type for function {self.unique_id}"
            ) from exc
        sensitive = spark.conf.get("spark.sql.caseSensitive", "false") == "true"
        names = [name if sensitive else name.lower() for name, _ in self.arguments]
        if len(names) != len(set(names)):
            raise DbtRuntimeError(f"Duplicate arguments for Spark function {self.unique_id}")


class SparkFunctionRegistry:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._definitions: Dict[str, SparkFunction] = {}
        self._sessions: Dict["SparkSession", Dict[str, SparkFunction]] = {}
        self._fingerprints: Dict["SparkSession", Dict[str, str]] = {}

    def require(self, dependencies: Sequence[str]) -> None:
        with self._lock:
            missing = [dep for dep in dependencies if dep not in self._definitions]
            if missing:
                raise DbtRuntimeError(
                    f"Spark session functions {', '.join(missing)} are not built in this "
                    "invocation. Select their resources with the consumer (for example, "
                    "dbt build --select +model_name); state deferral is not deployment."
                )

    def validate_names(self, spark: "SparkSession", models: Sequence[Mapping[str, Any]]) -> None:
        sensitive = spark.conf.get("spark.sql.caseSensitive", "false") == "true"
        names: Dict[str, str] = {}
        for model in models:
            identifier = (
                model.get("identifier")
                or model.get("alias")
                or model.get("config", {}).get("alias")
            )
            identifier = _identifier(identifier or model.get("name"), "name")
            key = identifier if sensitive else identifier.lower()
            uid = model["unique_id"]
            if key in names and names[key] != uid:
                raise DbtRuntimeError(
                    f"Spark session function name collision: {names[key]} and {uid}"
                )
            names[key] = uid

    def _register(self, spark: "SparkSession", function: SparkFunction) -> None:
        registered = self._sessions.setdefault(spark, {})
        fingerprints = self._fingerprints.setdefault(spark, {})
        sensitive = spark.conf.get("spark.sql.caseSensitive", "false") == "true"
        normalize = (lambda name: name) if sensitive else (lambda name: name.lower())
        key = normalize(function.identifier)
        previous = registered.get(key)
        dependency_fingerprints = [fingerprints.get(dep, "") for dep in function.dependencies]
        fingerprint = hashlib.sha256(
            json.dumps([function.fingerprint, dependency_fingerprints]).encode()
        ).hexdigest()
        if previous is not None and previous.unique_id != function.unique_id:
            raise DbtRuntimeError(f"Spark function name collision: {function.unique_id}")
        if previous is not None and fingerprints.get(function.unique_id) == fingerprint:
            return
        function.validate_session(spark)
        try:
            if previous is None and spark.catalog.functionExists(
                quote_identifier(function.identifier)
            ):
                raise DbtRuntimeError(
                    f"Spark function {function.unique_id} would overwrite an unowned function"
                )
            if function.language == "sql":
                spark.sql(function.sql()).collect()
            else:
                if function.entry_point is None:
                    raise DbtRuntimeError("Spark Python functions require an entry_point")
                namespace: Dict[str, Any] = {}
                exec(function.python_code(), namespace)
                callable_function = namespace[function.entry_point]
                if function.volatility == "non-deterministic":
                    from pyspark.sql.functions import udf

                    registered_udf = udf(
                        callable_function, function.return_type
                    ).asNondeterministic()
                    spark.udf.register(function.identifier, registered_udf)
                else:
                    spark.udf.register(
                        function.identifier, callable_function, function.return_type
                    )
        except DbtRuntimeError:
            raise
        except Exception as exc:
            from pyspark.errors import PySparkException

            condition = exc.getCondition() if isinstance(exc, PySparkException) else None
            raise DbtRuntimeError(
                f"Failed to register Spark function {function.unique_id} "
                f"({condition or type(exc).__name__}): {exc}"
            ) from exc
        registered[key] = function
        fingerprints[function.unique_id] = fingerprint

    def build(self, spark: "SparkSession", function: SparkFunction) -> None:
        with self._lock:
            self.require(function.dependencies)
            self.ensure(spark)
            self._register(spark, function)
            self._definitions[function.unique_id] = function

    def ensure(self, spark: "SparkSession") -> None:
        with self._lock:
            # Definitions enter this ordered mapping only after their dependencies succeed.
            for function in self._definitions.values():
                self._register(spark, function)

    def contains(self, unique_id: str) -> bool:
        with self._lock:
            return unique_id in self._definitions

    @property
    def has_definitions(self) -> bool:
        with self._lock:
            return bool(self._definitions)

    def cleanup(self) -> List[DbtRuntimeError]:
        errors = []
        with self._lock:
            for spark, functions in self._sessions.items():
                for function in reversed(list(functions.values())):
                    try:
                        spark.sql(
                            f"DROP TEMPORARY FUNCTION IF EXISTS "
                            f"{quote_identifier(function.identifier)}"
                        ).collect()
                    except Exception as exc:
                        error = DbtRuntimeError(
                            f"Failed to clean up Spark function {function.unique_id}"
                        )
                        error.__cause__ = exc
                        errors.append(error)
            self._definitions.clear()
            self._sessions.clear()
            self._fingerprints.clear()
        return errors
