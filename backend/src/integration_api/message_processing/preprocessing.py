"""M8 reusable, side-effect-free preprocessing pipeline.

Runtime persists its result afterwards; preview merely returns it.  This keeps
validation and preview behaviour identical by construction.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping

from jsonschema import ValidationError as JsonValidationError
from jsonschema.validators import validator_for
from lxml import etree

from integration_api.message_processing.contracts import ExternalEventEnvelope
from integration_api.message_processing.field_mapping import TransformationConfigurationError, TransformationError
from integration_api.message_processing.ports import IntegrationRoute, TargetPayload, ValidationSchemaConfiguration
from integration_api.message_processing.serialization import serialize_target_document
from integration_api.message_processing.transformation import RouteTransformationProvider


class ValidationConfigurationError(ValueError):
    """A stored JSON Schema/XSD cannot safely be used."""


@dataclass(frozen=True)
class SourceMaterial:
    """Optional transient wire representation kept only during preprocessing."""

    raw_format: str | None = None
    raw_body: bytes | None = None


@dataclass(frozen=True)
class StructuredDiagnostic:
    stage: str
    category: str
    message: str
    instance_path: str | None = None
    schema_path: str | None = None
    details: dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        result = {
            "stage": self.stage,
            "category": self.category,
            "message": self.message,
            "instance_path": self.instance_path,
            "schema_path": self.schema_path,
            "details": self.details,
        }
        return {key: value for key, value in result.items() if value not in (None, {}, "")}


@dataclass(frozen=True)
class PreprocessingResult:
    status: str
    stages: tuple[str, ...]
    canonical_target: dict[str, Any] | None = None
    payload: TargetPayload | None = None
    diagnostic: StructuredDiagnostic | None = None

    @property
    def success(self) -> bool:
        return self.status == "DELIVERY_REQUESTED"


class ContractValidator:
    """Validates self-contained JSON Schema and XSD contracts securely."""

    def validate_json(self, schema: ValidationSchemaConfiguration, value: Mapping[str, Any]) -> StructuredDiagnostic | None:
        definition = schema.definition_json
        if not isinstance(definition, dict):
            raise ValidationConfigurationError("JSON Schema definition must be an object")
        _reject_external_json_refs(definition)
        try:
            validator_class = validator_for(definition)
            validator_class.check_schema(definition)
            error = next(validator_class(definition).iter_errors(value), None)
        except Exception as exc:
            raise ValidationConfigurationError(f"Invalid JSON Schema: {exc}") from exc
        if error is None:
            return None
        assert isinstance(error, JsonValidationError)
        return StructuredDiagnostic(
            stage="source_validation" if schema.payload_format == "JSON" else "target_validation",
            category="JSON_SCHEMA_VALIDATION_FAILED",
            message=error.message,
            instance_path=_json_pointer(error.absolute_path),
            schema_path=_json_pointer(error.absolute_schema_path),
            details={"validator": error.validator},
        )

    def validate_definition(self, schema: ValidationSchemaConfiguration) -> None:
        """Compile/check a stored schema before it may become ACTIVE."""
        if schema.schema_type == "JSON_SCHEMA" and schema.payload_format == "JSON":
            if not isinstance(schema.definition_json, dict):
                raise ValidationConfigurationError("JSON Schema definition must be an object")
            _reject_external_json_refs(schema.definition_json)
            try:
                validator_for(schema.definition_json).check_schema(schema.definition_json)
            except Exception as exc:
                raise ValidationConfigurationError(f"Invalid JSON Schema: {exc}") from exc
            return
        if schema.schema_type == "XSD" and schema.payload_format == "XML":
            if not isinstance(schema.definition_text, str) or not schema.definition_text.strip():
                raise ValidationConfigurationError("XSD definition must be non-empty text")
            try:
                root = etree.fromstring(schema.definition_text.encode("utf-8"), parser=_secure_parser())
                if root.xpath("//*[local-name()='include' or local-name()='import']"):
                    raise ValidationConfigurationError("XSD include/import is not supported")
                etree.XMLSchema(root)
            except ValidationConfigurationError:
                raise
            except (etree.XMLSyntaxError, etree.XMLSchemaParseError) as exc:
                raise ValidationConfigurationError(f"Invalid XSD: {exc}") from exc
            return
        raise ValidationConfigurationError(f"Unsupported schema/format combination: {schema.schema_type}/{schema.payload_format}")

    def validate_xml(self, schema: ValidationSchemaConfiguration, raw_xml: bytes) -> StructuredDiagnostic | None:
        definition = schema.definition_text
        if not isinstance(definition, str) or not definition.strip():
            raise ValidationConfigurationError("XSD definition must be non-empty text")
        try:
            xsd_root = etree.fromstring(definition.encode("utf-8"), parser=_secure_parser())
            if xsd_root.xpath("//*[local-name()='include' or local-name()='import']"):
                raise ValidationConfigurationError("XSD include/import is not supported")
            validator = etree.XMLSchema(xsd_root)
        except ValidationConfigurationError:
            raise
        except (etree.XMLSyntaxError, etree.XMLSchemaParseError) as exc:
            raise ValidationConfigurationError(f"Invalid XSD: {exc}") from exc
        try:
            document = etree.fromstring(raw_xml, parser=_secure_parser())
        except etree.XMLSyntaxError as exc:
            return StructuredDiagnostic("source_validation", "XML_SYNTAX_INVALID", str(exc))
        if validator.validate(document):
            return None
        error = validator.error_log.last_error
        return StructuredDiagnostic(
            stage="source_validation" if schema.payload_format == "XML" else "target_validation",
            category="XSD_VALIDATION_FAILED",
            message=error.message if error is not None else "XML does not satisfy XSD",
            instance_path=error.path if error is not None else None,
            details={"line": error.line} if error is not None else {},
        )

    def validate(self, schema: ValidationSchemaConfiguration, canonical: Mapping[str, Any], raw_xml: bytes | None = None) -> StructuredDiagnostic | None:
        if schema.schema_type == "JSON_SCHEMA" and schema.payload_format == "JSON":
            return self.validate_json(schema, canonical)
        if schema.schema_type == "XSD" and schema.payload_format == "XML":
            if raw_xml is None:
                raise ValidationConfigurationError("XSD validation requires actual XML bytes")
            return self.validate_xml(schema, raw_xml)
        raise ValidationConfigurationError(
            f"Unsupported schema/format combination: {schema.schema_type}/{schema.payload_format}"
        )


class PreprocessingPipeline:
    """One M8 pipeline shared by production processing and preview."""

    def __init__(self, transformer: RouteTransformationProvider, validator: ContractValidator | None = None) -> None:
        self._transformer = transformer
        self._validator = validator or ContractValidator()

    def execute(
        self, route: IntegrationRoute, event: ExternalEventEnvelope, source_material: SourceMaterial | None = None
    ) -> PreprocessingResult:
        stages: list[str] = []
        if route.configuration_error is not None:
            return _configuration_failure(stages, route.configuration_error)
        if route.source_validation_schema is None or route.target_validation_schema is None:
            return _configuration_failure(stages, "Active route must have source and target validation schemas")

        try:
            source_error = self._validator.validate(
                route.source_validation_schema,
                event.payload,
                source_material.raw_body if source_material and source_material.raw_format == "XML" else None,
            )
        except ValidationConfigurationError as exc:
            return _configuration_failure(stages, str(exc), "source_validation")
        stages.append("source_validation")
        if source_error is not None:
            return PreprocessingResult("SOURCE_VALIDATION_FAILED", tuple(stages), diagnostic=_with_stage(source_error, "source_validation"))

        try:
            canonical_target = self._transformer.transform_document(route, event)
        except TransformationConfigurationError as exc:
            return _configuration_failure(stages, str(exc), "transformation")
        except TransformationError as exc:
            return PreprocessingResult(
                "TRANSFORMATION_FAILED", tuple(stages + ["transformation"]),
                diagnostic=StructuredDiagnostic("transformation", exc.category, str(exc), details=exc.as_dict()),
            )
        stages.append("transformation")

        try:
            if route.target_format == "JSON":
                target_error = self._validator.validate(route.target_validation_schema, canonical_target)
                stages.append("target_validation")
                if target_error is not None:
                    return PreprocessingResult("TARGET_VALIDATION_FAILED", tuple(stages), canonical_target, diagnostic=_with_stage(target_error, "target_validation"))
                payload = serialize_target_document(route.target_format, canonical_target)
                stages.append("serialization")
            elif route.target_format == "XML":
                payload = serialize_target_document(route.target_format, canonical_target)
                stages.append("serialization")
                assert isinstance(payload.body, str)
                target_error = self._validator.validate(route.target_validation_schema, canonical_target, payload.body.encode("utf-8"))
                stages.append("target_validation")
                if target_error is not None:
                    return PreprocessingResult("TARGET_VALIDATION_FAILED", tuple(stages), canonical_target, diagnostic=_with_stage(target_error, "target_validation"))
            else:
                return _configuration_failure(stages, f"Unsupported target format: {route.target_format}")
        except ValidationConfigurationError as exc:
            return _configuration_failure(stages, str(exc), "target_validation")
        except TransformationConfigurationError as exc:
            return _configuration_failure(stages, str(exc), "serialization")
        return PreprocessingResult("DELIVERY_REQUESTED", tuple(stages), canonical_target, payload)


def _configuration_failure(stages: list[str], message: str, stage: str = "configuration") -> PreprocessingResult:
    return PreprocessingResult("CONFIGURATION_ERROR", tuple(stages), diagnostic=StructuredDiagnostic(stage, "CONFIGURATION_INVALID", message))


def _with_stage(diagnostic: StructuredDiagnostic, stage: str) -> StructuredDiagnostic:
    return StructuredDiagnostic(stage, diagnostic.category, diagnostic.message, diagnostic.instance_path, diagnostic.schema_path, diagnostic.details)


def _secure_parser() -> etree.XMLParser:
    return etree.XMLParser(resolve_entities=False, no_network=True, load_dtd=False, dtd_validation=False, huge_tree=False)


def _reject_external_json_refs(value: Any) -> None:
    if isinstance(value, dict):
        reference = value.get("$ref")
        if isinstance(reference, str) and not reference.startswith("#"):
            raise ValidationConfigurationError("External JSON Schema $ref is not supported")
        for item in value.values():
            _reject_external_json_refs(item)
    elif isinstance(value, list):
        for item in value:
            _reject_external_json_refs(item)


def _json_pointer(parts: Any) -> str:
    return "/" + "/".join(str(part).replace("~", "~0").replace("/", "~1") for part in parts) if parts else "/"
