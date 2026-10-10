from decimal import Decimal

import pytest

from integration_api.message_processing.field_mapping import (
    FieldMappingV1Engine,
    TransformationConfigurationError,
    TransformationRuntimeError,
)


def _engine() -> FieldMappingV1Engine:
    return FieldMappingV1Engine()


def _definition(target: dict) -> dict:
    return {"target": target}


def test_from_supports_root_nested_and_map_variable_paths() -> None:
    definition = _definition({
        "id": {"op": "from", "path": "$.id"},
        "nested": {"email": {"op": "from", "path": "$.customer.email"}},
        "items": {"op": "map", "path": "$.items", "as": "item", "target": {"sku": {"op": "from", "path": "$item.sku"}, "order": {"op": "from", "path": "$.id"}}},
    })
    assert _engine().transform(definition, {"id": "o-1", "customer": {"email": "a@b.test"}, "items": [{"sku": "A"}]}) == {
        "id": "o-1", "nested": {"email": "a@b.test"}, "items": [{"sku": "A", "order": "o-1"}]
    }


def test_from_distinguishes_missing_path_from_existing_null() -> None:
    definition = _definition({"present": {"op": "from", "path": "$.present"}, "missing": {"op": "from", "path": "$.missing"}})
    with pytest.raises(TransformationRuntimeError) as error:
        _engine().transform(definition, {"present": None})
    assert error.value.category == "SOURCE_PATH_NOT_FOUND"
    assert error.value.source_path == "$.missing"
    assert error.value.target_path == "$.missing"


@pytest.mark.parametrize("value", ["", 0, False])
def test_default_preserves_present_falsey_values(value: object) -> None:
    definition = _definition({"value": {"op": "default", "value": {"op": "from", "path": "$.value"}, "fallback": {"op": "literal", "value": "fallback"}}})
    assert _engine().transform(definition, {"value": value}) == {"value": value}


@pytest.mark.parametrize("source", [{}, {"value": None}])
def test_default_uses_fallback_for_missing_or_null(source: dict) -> None:
    definition = _definition({"value": {"op": "default", "value": {"op": "from", "path": "$.value"}, "fallback": {"op": "literal", "value": "fallback"}}})
    assert _engine().transform(definition, source) == {"value": "fallback"}


def test_concat_uses_controlled_scalar_conversion() -> None:
    definition = _definition({"text": {"op": "concat", "values": [{"op": "literal", "value": "id-"}, {"op": "from", "path": "$.id"}, {"op": "literal", "value": True}]}})
    assert _engine().transform(definition, {"id": 7}) == {"text": "id-7true"}


@pytest.mark.parametrize("value", [None, {}, []])
def test_concat_rejects_null_and_composite_values(value: object) -> None:
    definition = _definition({"text": {"op": "concat", "values": [{"op": "from", "path": "$.value"}]}})
    with pytest.raises(TransformationRuntimeError) as error:
        _engine().transform(definition, {"value": value})
    assert error.value.category == "TYPE_MISMATCH"


def test_map_preserves_order_and_supports_nested_maps_with_lexical_context() -> None:
    definition = _definition({"groups": {"op": "map", "path": "$.groups", "as": "group", "target": {"name": {"op": "from", "path": "$group.name"}, "lines": {"op": "map", "path": "$group.lines", "as": "line", "target": {"sku": {"op": "from", "path": "$line.sku"}, "group": {"op": "from", "path": "$group.name"}, "root": {"op": "from", "path": "$.order"}}}}}})
    source = {"order": "o-1", "groups": [{"name": "first", "lines": [{"sku": "A"}]}, {"name": "second", "lines": [{"sku": "B"}]}]}
    assert _engine().transform(definition, source) == {"groups": [{"name": "first", "lines": [{"sku": "A", "group": "first", "root": "o-1"}]}, {"name": "second", "lines": [{"sku": "B", "group": "second", "root": "o-1"}]}]}


def test_map_requires_array() -> None:
    definition = _definition({"items": {"op": "map", "path": "$.items", "as": "item", "target": {"id": {"op": "from", "path": "$item.id"}}}})
    with pytest.raises(TransformationRuntimeError) as error:
        _engine().transform(definition, {"items": {}})
    assert error.value.category == "TYPE_MISMATCH"


def test_map_keeps_an_empty_array() -> None:
    definition = _definition({"items": {"op": "map", "path": "$.items", "as": "item", "target": {"id": {"op": "from", "path": "$item.id"}}}})
    assert _engine().transform(definition, {"items": []}) == {"items": []}


@pytest.mark.parametrize(
    ("operation", "value", "expected"),
    [("to_string", Decimal("12.50"), "12.50"), ("to_integer", "42", 42), ("to_decimal", "12.50", Decimal("12.50")), ("to_boolean", "false", False)],
)
def test_explicit_conversions(operation: str, value: object, expected: object) -> None:
    definition = _definition({"value": {"op": operation, "value": {"op": "from", "path": "$.value"}}})
    assert _engine().transform(definition, {"value": value}) == {"value": expected}


@pytest.mark.parametrize("operation,value", [("to_integer", "1.5"), ("to_integer", Decimal("1.5")), ("to_decimal", "wrong"), ("to_boolean", "FALSE"), ("to_string", None)])
def test_invalid_conversion_is_structured(operation: str, value: object) -> None:
    definition = _definition({"value": {"op": operation, "value": {"op": "from", "path": "$.value"}}})
    with pytest.raises(TransformationRuntimeError) as error:
        _engine().transform(definition, {"value": value})
    assert error.value.category in {"CONVERSION_FAILED", "TYPE_MISMATCH"}
    assert error.value.operation == operation


def test_if_is_lazy_and_conditions_are_strict() -> None:
    definition = _definition({"value": {"op": "if", "condition": {"op": "equals", "left": {"op": "from", "path": "$.kind"}, "right": {"op": "literal", "value": "ok"}}, "then": {"op": "literal", "value": "yes"}, "else": {"op": "from", "path": "$.only_for_else"}}})
    assert _engine().transform(definition, {"kind": "ok"}) == {"value": "yes"}
    assert _engine().transform(definition, {"kind": "no", "only_for_else": "fallback"}) == {"value": "fallback"}


def test_exists_treats_null_as_not_existing_and_omit_removes_field() -> None:
    definition = _definition({"phone": {"op": "if", "condition": {"op": "exists", "path": "$.phone"}, "then": {"op": "from", "path": "$.phone"}, "else": {"op": "omit"}}, "explicit_null": {"op": "literal", "value": None}})
    assert _engine().transform(definition, {"phone": None}) == {"explicit_null": None}
    assert _engine().transform(definition, {"phone": "123"}) == {"phone": "123", "explicit_null": None}


def test_logical_conditions_short_circuit() -> None:
    definition = _definition({"value": {"op": "if", "condition": {"op": "or", "values": [{"op": "equals", "left": {"op": "literal", "value": "yes"}, "right": {"op": "literal", "value": "yes"}}, {"op": "equals", "left": {"op": "from", "path": "$.missing"}, "right": {"op": "literal", "value": "x"}}]}, "then": {"op": "literal", "value": "ok"}, "else": {"op": "literal", "value": "no"}}})
    assert _engine().transform(definition, {}) == {"value": "ok"}


def test_not_equals_and_not_do_not_apply_implicit_type_coercion() -> None:
    definition = _definition({
        "equal": {"op": "if", "condition": {"op": "equals", "left": {"op": "literal", "value": "1"}, "right": {"op": "literal", "value": 1}}, "then": {"op": "literal", "value": True}, "else": {"op": "literal", "value": False}},
        "not_equal": {"op": "if", "condition": {"op": "not_equals", "left": {"op": "literal", "value": "1"}, "right": {"op": "literal", "value": 1}}, "then": {"op": "literal", "value": True}, "else": {"op": "literal", "value": False}},
        "not": {"op": "if", "condition": {"op": "not", "value": {"op": "exists", "path": "$.missing"}}, "then": {"op": "literal", "value": "yes"}, "else": {"op": "literal", "value": "no"}},
        "and": {"op": "if", "condition": {"op": "and", "values": [{"op": "exists", "path": "$.present"}, {"op": "equals", "left": {"op": "from", "path": "$.present"}, "right": {"op": "literal", "value": "ok"}}]}, "then": {"op": "literal", "value": "yes"}, "else": {"op": "literal", "value": "no"}},
    })
    assert _engine().transform(definition, {"present": "ok"}) == {"equal": False, "not_equal": True, "not": "yes", "and": "yes"}


@pytest.mark.parametrize(
    "definition",
    [
        {"target": {"value": {"op": "eval", "value": "1 + 1"}}},
        {"target": {"value": {"op": "http", "url": "http://target"}}},
        {"target": {"value": {"op": "sql", "query": "SELECT 1"}}},
        {"target": {"value": {"op": "environment", "name": "SECRET"}}},
        {"target": {"value": {"op": "filesystem", "path": "/tmp/data"}}},
        {"target": {"value": {"op": "from", "path": "$.items[*]"}}},
        {"target": {"value": {"op": "map", "path": "$.items", "as": "1bad", "target": {}}}},
        {"target": {"value": {"op": "and", "values": []}}},
    ],
)
def test_malformed_or_unsafe_definitions_are_rejected(definition: dict) -> None:
    with pytest.raises(TransformationConfigurationError) as error:
        _engine().transform(definition, {"items": []})
    assert error.value.category == "CONFIGURATION_INVALID"
