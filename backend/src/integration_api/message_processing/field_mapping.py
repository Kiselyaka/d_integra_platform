"""Safe, declarative FIELD_MAPPING_V1 transformation engine.

The module deliberately supports a small typed AST rather than JSONPath or
user-provided code.  It has no I/O dependencies and is therefore deterministic
for one definition and one canonical source document.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
import re
from typing import Any, Mapping, TypeAlias


class TransformationError(RuntimeError):
    """A structured error suitable for a RouteProcessingResult."""

    def __init__(
        self,
        category: str,
        message: str,
        *,
        operation: str | None = None,
        source_path: str | None = None,
        target_path: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.category = category
        self.message = message
        self.operation = operation
        self.source_path = source_path
        self.target_path = target_path
        self.details = details or {}

    def as_dict(self) -> dict[str, Any]:
        return {
            "category": self.category,
            "message": self.message,
            "operation": self.operation,
            "source_path": self.source_path,
            "target_path": self.target_path,
            "details": self.details,
        }


class TransformationConfigurationError(TransformationError):
    def __init__(self, message: str, **kwargs: Any) -> None:
        super().__init__("CONFIGURATION_INVALID", message, **kwargs)


class TransformationRuntimeError(TransformationError):
    pass


class _Missing:
    pass


class _Omit:
    pass


MISSING = _Missing()
OMIT = _Omit()
_PATH_PART = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
_VARIABLE_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
_INTEGER = re.compile(r"[+-]?\d+")


class PathResolver:
    """Resolves the intentionally limited `$` / `$variable` path grammar."""

    @staticmethod
    def validate(path: object) -> str:
        if not isinstance(path, str) or not path.startswith("$"):
            raise TransformationConfigurationError("Path must start with `$`", operation="path")
        head, *tail = path[1:].split(".")
        if head and not _VARIABLE_NAME.fullmatch(head):
            raise TransformationConfigurationError(f"Invalid path root: {path}", operation="path", source_path=path)
        if any(not _PATH_PART.fullmatch(part) for part in tail):
            raise TransformationConfigurationError(f"Invalid path syntax: {path}", operation="path", source_path=path)
        return path

    def resolve(self, path: str, root: Mapping[str, Any], variables: Mapping[str, Any]) -> Any:
        value = self.resolve_optional(path, root, variables)
        if value is MISSING:
            raise TransformationRuntimeError(
                "SOURCE_PATH_NOT_FOUND",
                f"Source path was not found: {path}",
                operation="from",
                source_path=path,
            )
        return value

    def resolve_optional(self, path: str, root: Mapping[str, Any], variables: Mapping[str, Any]) -> Any:
        self.validate(path)
        head, *parts = path[1:].split(".")
        value: Any = root if not head else variables.get(head, MISSING)
        for part in parts:
            if not isinstance(value, Mapping) or part not in value:
                return MISSING
            value = value[part]
        return value


@dataclass(frozen=True)
class FromExpression:
    path: str


@dataclass(frozen=True)
class LiteralExpression:
    value: str | int | float | bool | None


@dataclass(frozen=True)
class DefaultExpression:
    value: "TargetNode"
    fallback: "TargetNode"


@dataclass(frozen=True)
class ConcatExpression:
    values: tuple["TargetNode", ...]


@dataclass(frozen=True)
class MapExpression:
    path: str
    variable: str
    target: "TargetNode"


@dataclass(frozen=True)
class ConversionExpression:
    operation: str
    value: "TargetNode"


@dataclass(frozen=True)
class IfExpression:
    condition: "Condition"
    then_value: "TargetNode"
    else_value: "TargetNode"


@dataclass(frozen=True)
class OmitExpression:
    pass


Expression: TypeAlias = (
    FromExpression
    | LiteralExpression
    | DefaultExpression
    | ConcatExpression
    | MapExpression
    | ConversionExpression
    | IfExpression
    | OmitExpression
)


@dataclass(frozen=True)
class TargetObject:
    fields: dict[str, "TargetNode"]


@dataclass(frozen=True)
class TargetArray:
    values: tuple["TargetNode", ...]


TargetNode: TypeAlias = Expression | TargetObject | TargetArray


@dataclass(frozen=True)
class ExistsCondition:
    path: str


@dataclass(frozen=True)
class ComparisonCondition:
    operation: str
    left: TargetNode
    right: TargetNode


@dataclass(frozen=True)
class LogicalCondition:
    operation: str
    values: tuple["Condition", ...]


@dataclass(frozen=True)
class NotCondition:
    value: "Condition"


Condition: TypeAlias = ExistsCondition | ComparisonCondition | LogicalCondition | NotCondition


@dataclass(frozen=True)
class FieldMappingDefinition:
    target: TargetObject


class FieldMappingV1DefinitionParser:
    """Parses one strictly-shaped JSON definition into typed AST nodes."""

    _expression_operations = {"from", "literal", "default", "concat", "map", "to_string", "to_integer", "to_decimal", "to_boolean", "if", "omit"}

    def parse(self, raw: object) -> FieldMappingDefinition:
        if not isinstance(raw, Mapping) or set(raw) != {"target"}:
            raise TransformationConfigurationError("FIELD_MAPPING_V1 definition must contain exactly `target`")
        target = self._target_node(raw["target"], "$")
        if not isinstance(target, TargetObject):
            raise TransformationConfigurationError("FIELD_MAPPING_V1 root target must be an object", target_path="$")
        return FieldMappingDefinition(target)

    def _target_node(self, raw: object, target_path: str) -> TargetNode:
        if isinstance(raw, Mapping):
            if "op" in raw:
                return self._expression(raw, target_path)
            fields: dict[str, TargetNode] = {}
            for name, child in raw.items():
                if not isinstance(name, str) or not name:
                    raise TransformationConfigurationError("Target object field names must be non-empty strings", target_path=target_path)
                fields[name] = self._target_node(child, f"{target_path}.{name}")
            return TargetObject(fields)
        if isinstance(raw, list):
            return TargetArray(tuple(self._target_node(value, f"{target_path}[{index}]") for index, value in enumerate(raw)))
        raise TransformationConfigurationError("Target scalar values must use an explicit literal expression", target_path=target_path)

    def _expression(self, raw: Mapping[str, Any], target_path: str) -> Expression:
        operation = raw.get("op")
        if operation not in self._expression_operations:
            raise TransformationConfigurationError(f"Unsupported operation: {operation}", operation=str(operation), target_path=target_path)
        if operation == "from":
            self._keys(raw, {"op", "path"}, target_path)
            return FromExpression(PathResolver.validate(raw.get("path")))
        if operation == "literal":
            self._keys(raw, {"op", "value"}, target_path)
            value = raw.get("value")
            if value is not None and (not isinstance(value, (str, int, float, bool)) or isinstance(value, (dict, list))):
                raise TransformationConfigurationError("Literal must be a scalar or null", operation="literal", target_path=target_path)
            return LiteralExpression(value)
        if operation == "default":
            self._keys(raw, {"op", "value", "fallback"}, target_path)
            return DefaultExpression(self._target_node(raw.get("value"), target_path), self._target_node(raw.get("fallback"), target_path))
        if operation == "concat":
            self._keys(raw, {"op", "values"}, target_path)
            values = raw.get("values")
            if not isinstance(values, list) or not values:
                raise TransformationConfigurationError("concat requires a non-empty values array", operation="concat", target_path=target_path)
            return ConcatExpression(tuple(self._target_node(value, target_path) for value in values))
        if operation == "map":
            self._keys(raw, {"op", "path", "as", "target"}, target_path)
            variable = raw.get("as")
            if not isinstance(variable, str) or not _VARIABLE_NAME.fullmatch(variable) or variable == "event":
                raise TransformationConfigurationError("map.as must be a valid non-reserved variable name", operation="map", target_path=target_path)
            return MapExpression(PathResolver.validate(raw.get("path")), variable, self._target_node(raw.get("target"), target_path))
        if operation in {"to_string", "to_integer", "to_decimal", "to_boolean"}:
            self._keys(raw, {"op", "value"}, target_path)
            return ConversionExpression(operation, self._target_node(raw.get("value"), target_path))
        if operation == "if":
            self._keys(raw, {"op", "condition", "then", "else"}, target_path)
            return IfExpression(self._condition(raw.get("condition"), target_path), self._target_node(raw.get("then"), target_path), self._target_node(raw.get("else"), target_path))
        self._keys(raw, {"op"}, target_path)
        return OmitExpression()

    def _condition(self, raw: object, target_path: str) -> Condition:
        if not isinstance(raw, Mapping):
            raise TransformationConfigurationError("Condition must be an object", target_path=target_path)
        operation = raw.get("op")
        if operation == "exists":
            self._keys(raw, {"op", "path"}, target_path)
            return ExistsCondition(PathResolver.validate(raw.get("path")))
        if operation in {"equals", "not_equals"}:
            self._keys(raw, {"op", "left", "right"}, target_path)
            return ComparisonCondition(operation, self._target_node(raw.get("left"), target_path), self._target_node(raw.get("right"), target_path))
        if operation in {"and", "or"}:
            self._keys(raw, {"op", "values"}, target_path)
            values = raw.get("values")
            if not isinstance(values, list) or not values:
                raise TransformationConfigurationError(f"{operation} requires a non-empty values array", operation=operation, target_path=target_path)
            return LogicalCondition(operation, tuple(self._condition(value, target_path) for value in values))
        if operation == "not":
            self._keys(raw, {"op", "value"}, target_path)
            return NotCondition(self._condition(raw.get("value"), target_path))
        raise TransformationConfigurationError(f"Unsupported condition operation: {operation}", operation=str(operation), target_path=target_path)

    @staticmethod
    def _keys(raw: Mapping[str, Any], expected: set[str], target_path: str) -> None:
        if set(raw) != expected:
            raise TransformationConfigurationError(
                f"Invalid operation shape; expected keys {sorted(expected)}", target_path=target_path
            )


class FieldMappingV1Engine:
    """Evaluates typed FIELD_MAPPING_V1 AST without integration knowledge."""

    def __init__(self, parser: FieldMappingV1DefinitionParser | None = None, resolver: PathResolver | None = None) -> None:
        self._parser = parser or FieldMappingV1DefinitionParser()
        self._resolver = resolver or PathResolver()

    def transform(
        self, definition: object, source_document: Mapping[str, Any], context: Mapping[str, Any] | None = None
    ) -> dict[str, Any]:
        parsed = self._parser.parse(definition)
        value = self._evaluate_node(parsed.target, source_document, dict(context or {}), "$")
        assert isinstance(value, dict)
        return value

    def _evaluate_node(self, node: TargetNode, root: Mapping[str, Any], variables: dict[str, Any], target_path: str) -> Any:
        if isinstance(node, TargetObject):
            result: dict[str, Any] = {}
            for name, child in node.fields.items():
                value = self._evaluate_node(child, root, variables, f"{target_path}.{name}")
                if value is not OMIT:
                    result[name] = value
            return result
        if isinstance(node, TargetArray):
            result = [self._evaluate_node(child, root, variables, f"{target_path}[{index}]") for index, child in enumerate(node.values)]
            if OMIT in result:
                raise TransformationRuntimeError("TYPE_MISMATCH", "omit is not supported as an array item", operation="omit", target_path=target_path)
            return result
        if isinstance(node, FromExpression):
            try:
                return self._resolver.resolve(node.path, root, variables)
            except TransformationRuntimeError as error:
                error.target_path = target_path
                raise
        if isinstance(node, LiteralExpression):
            return node.value
        if isinstance(node, DefaultExpression):
            if isinstance(node.value, FromExpression):
                value = self._resolver.resolve_optional(node.value.path, root, variables)
                if value is MISSING:
                    return self._evaluate_node(node.fallback, root, variables, target_path)
            else:
                value = self._evaluate_node(node.value, root, variables, target_path)
            return self._evaluate_node(node.fallback, root, variables, target_path) if value is None else value
        if isinstance(node, ConcatExpression):
            return "".join(self._to_text(self._evaluate_node(value, root, variables, target_path), "concat", target_path) for value in node.values)
        if isinstance(node, MapExpression):
            try:
                source = self._resolver.resolve(node.path, root, variables)
            except TransformationRuntimeError as error:
                error.target_path = target_path
                raise
            if not isinstance(source, list):
                raise TransformationRuntimeError("TYPE_MISMATCH", "map source must be an array", operation="map", source_path=node.path, target_path=target_path)
            result = []
            for index, item in enumerate(source):
                nested_variables = {**variables, node.variable: item}
                value = self._evaluate_node(node.target, root, nested_variables, f"{target_path}[{index}]")
                if value is OMIT:
                    raise TransformationRuntimeError("TYPE_MISMATCH", "omit is not supported as a map item", operation="map", target_path=f"{target_path}[{index}]")
                result.append(value)
            return result
        if isinstance(node, ConversionExpression):
            return self._convert(node.operation, self._evaluate_node(node.value, root, variables, target_path), target_path)
        if isinstance(node, IfExpression):
            branch = node.then_value if self._evaluate_condition(node.condition, root, variables, target_path) else node.else_value
            return self._evaluate_node(branch, root, variables, target_path)
        if isinstance(node, OmitExpression):
            return OMIT
        raise AssertionError(f"Unhandled AST node: {type(node).__name__}")

    def _evaluate_condition(self, condition: Condition, root: Mapping[str, Any], variables: dict[str, Any], target_path: str) -> bool:
        if isinstance(condition, ExistsCondition):
            value = self._resolver.resolve_optional(condition.path, root, variables)
            return value is not MISSING and value is not None
        if isinstance(condition, ComparisonCondition):
            left = self._evaluate_node(condition.left, root, variables, target_path)
            right = self._evaluate_node(condition.right, root, variables, target_path)
            if not self._is_scalar(left) or not self._is_scalar(right):
                raise TransformationRuntimeError("CONDITION_EVALUATION_FAILED", "Comparison operands must be scalar", operation=condition.operation, target_path=target_path)
            equal = type(left) is type(right) and left == right
            return equal if condition.operation == "equals" else not equal
        if isinstance(condition, LogicalCondition):
            if condition.operation == "and":
                return all(self._evaluate_condition(value, root, variables, target_path) for value in condition.values)
            return any(self._evaluate_condition(value, root, variables, target_path) for value in condition.values)
        if isinstance(condition, NotCondition):
            return not self._evaluate_condition(condition.value, root, variables, target_path)
        raise AssertionError(f"Unhandled condition: {type(condition).__name__}")

    @staticmethod
    def _is_scalar(value: Any) -> bool:
        return value is None or (isinstance(value, (str, int, float, bool, Decimal)) and not isinstance(value, (dict, list)))

    def _to_text(self, value: Any, operation: str, target_path: str) -> str:
        if value is None or isinstance(value, (dict, list)) or value is OMIT:
            raise TransformationRuntimeError("TYPE_MISMATCH", "Expected a non-null scalar value", operation=operation, target_path=target_path)
        if isinstance(value, bool):
            return "true" if value else "false"
        if isinstance(value, Decimal):
            return format(value, "f")
        return str(value)

    def _convert(self, operation: str, value: Any, target_path: str) -> Any:
        if operation == "to_string":
            return self._to_text(value, operation, target_path)
        if operation == "to_integer":
            if isinstance(value, bool):
                raise TransformationRuntimeError("CONVERSION_FAILED", "Boolean cannot be converted to integer", operation=operation, target_path=target_path)
            if isinstance(value, int):
                return value
            if isinstance(value, Decimal):
                if value != value.to_integral_value():
                    raise TransformationRuntimeError("CONVERSION_FAILED", "Decimal has a fractional part", operation=operation, target_path=target_path)
                return int(value)
            if isinstance(value, str) and _INTEGER.fullmatch(value):
                return int(value)
            raise TransformationRuntimeError("CONVERSION_FAILED", "Value cannot be converted to integer", operation=operation, target_path=target_path)
        if operation == "to_decimal":
            if isinstance(value, bool) or isinstance(value, (dict, list)) or value is None:
                raise TransformationRuntimeError("CONVERSION_FAILED", "Value cannot be converted to decimal", operation=operation, target_path=target_path)
            try:
                result = value if isinstance(value, Decimal) else Decimal(str(value))
            except (InvalidOperation, ValueError) as error:
                raise TransformationRuntimeError("CONVERSION_FAILED", "Value cannot be converted to decimal", operation=operation, target_path=target_path) from error
            if not result.is_finite():
                raise TransformationRuntimeError("CONVERSION_FAILED", "Decimal must be finite", operation=operation, target_path=target_path)
            return result
        if operation == "to_boolean":
            if isinstance(value, bool):
                return value
            if isinstance(value, str) and value in {"true", "false"}:
                return value == "true"
            raise TransformationRuntimeError("CONVERSION_FAILED", "Value must be boolean or `true`/`false`", operation=operation, target_path=target_path)
        raise AssertionError(f"Unhandled conversion: {operation}")
