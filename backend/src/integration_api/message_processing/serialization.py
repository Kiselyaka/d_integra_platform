"""Wire-format serializers for canonical transformation output."""

from __future__ import annotations

from decimal import Decimal
from typing import Any, Mapping
from xml.etree import ElementTree

from integration_api.message_processing.field_mapping import TransformationConfigurationError
from integration_api.message_processing.ports import TargetPayload


def serialize_target_document(target_format: str, document: Mapping[str, Any]) -> TargetPayload:
    """Convert an engine-neutral canonical document to the configured wire format."""

    if target_format == "JSON":
        return TargetPayload("application/json", dict(document))
    if target_format == "XML":
        return TargetPayload("application/xml; charset=utf-8", serialize_xml_document(document))
    raise TransformationConfigurationError(f"Unsupported target format: {target_format}")


def serialize_xml_document(document: Mapping[str, Any]) -> str:
    """Serialize a one-root canonical XML document without integration branches.

    A list under a field emits repeated elements with that field name. Objects
    create nested elements; scalar values become element text.
    """

    if set(document) == {"xml"}:
        return _serialize_ordered_xml(document["xml"])
    if len(document) != 1:
        raise TransformationConfigurationError("XML target document must have exactly one root element")
    root_name, root_value = next(iter(document.items()))
    if not isinstance(root_name, str) or not root_name or not isinstance(root_value, Mapping):
        raise TransformationConfigurationError("XML root must be a named object")
    root = ElementTree.Element(root_name)
    _append_xml_value(root, root_value)
    return ElementTree.tostring(root, encoding="utf-8", xml_declaration=True).decode("utf-8")


def _serialize_ordered_xml(root_descriptor: Any) -> str:
    if not isinstance(root_descriptor, Mapping):
        raise TransformationConfigurationError("Ordered XML root must be an object")
    root = _xml_element(root_descriptor)
    return ElementTree.tostring(root, encoding="utf-8", xml_declaration=True).decode("utf-8")


def _xml_element(descriptor: Mapping[str, Any]) -> ElementTree.Element:
    name = descriptor.get("name")
    if not isinstance(name, str) or not name:
        raise TransformationConfigurationError("Ordered XML element requires a non-empty name")
    keys = set(descriptor)
    if keys not in ({"name", "text"}, {"name", "children"}, {"name"}):
        raise TransformationConfigurationError("Ordered XML element must use text or children")
    element = ElementTree.Element(name)
    if "text" in descriptor:
        element.text = _xml_scalar(descriptor["text"])
    if "children" in descriptor:
        children = descriptor["children"]
        if not isinstance(children, list):
            raise TransformationConfigurationError("Ordered XML children must be an array")
        for child in children:
            if not isinstance(child, Mapping):
                raise TransformationConfigurationError("Ordered XML child must be an object")
            element.append(_xml_element(child))
    return element


def _append_xml_value(parent: ElementTree.Element, value: Mapping[str, Any]) -> None:
    for name, child_value in value.items():
        if not isinstance(name, str) or not name:
            raise TransformationConfigurationError("XML element names must be non-empty strings")
        values = child_value if isinstance(child_value, list) else [child_value]
        for item in values:
            child = ElementTree.SubElement(parent, name)
            if isinstance(item, Mapping):
                _append_xml_value(child, item)
            elif item is not None:
                child.text = _xml_scalar(item)


def _xml_scalar(value: Any) -> str:
    if isinstance(value, (dict, list)):
        raise TransformationConfigurationError("XML leaf values must be scalar")
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, Decimal):
        return format(value, "f")
    return str(value)
