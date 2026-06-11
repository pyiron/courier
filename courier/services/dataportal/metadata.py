"""Dataportal-specific adaptation of publication metadata."""

from __future__ import annotations

import json
import re
import unicodedata
from dataclasses import dataclass, field
from typing import Any

from courier.exceptions import ValidationError
from courier.metadata import Contributor, Person, PublicationMetadata, RelatedIdentifier


@dataclass
class DataportalMetadata:
    """Adapt service-neutral publication metadata to a CKAN package payload."""

    metadata: PublicationMetadata
    name: str | None = None
    owner_org: str | None = None
    private: bool | None = None
    groups: list[str] = field(default_factory=list)
    extras: dict[str, str] = field(default_factory=dict)
    dataset_type: str | None = None

    def __post_init__(self) -> None:
        self.validate()

    def validate(self) -> None:
        """Validate Dataportal-specific fields and adapter boundaries."""
        if not isinstance(self.metadata, PublicationMetadata):
            raise ValidationError("metadata must be a PublicationMetadata instance")

        if self.name is not None:
            _ = _required_string(self.name, "name")
        if self.owner_org is not None:
            _ = _required_string(self.owner_org, "owner_org")
        if self.dataset_type is not None:
            _ = _required_string(self.dataset_type, "dataset_type")
        if self.private is not None and not isinstance(self.private, bool):
            raise ValidationError("private must be a boolean when provided")

        if not isinstance(self.groups, list):
            raise ValidationError("groups must be a list")
        for group in self.groups:
            _ = _required_string(group, "group")

        if not isinstance(self.extras, dict):
            raise ValidationError("extras must be a dict")
        normalized_extra_keys: set[str] = set()
        for key, value in self.extras.items():
            normalized_key = _required_string(key, "extra key")
            if normalized_key in normalized_extra_keys:
                raise ValidationError(
                    f"duplicate extra key after normalization: {normalized_key!r}"
                )
            normalized_extra_keys.add(normalized_key)
            if not isinstance(value, str):
                raise ValidationError("extra values must be strings")

        conflicts = sorted(normalized_extra_keys & _generated_extra_keys(self.metadata))
        if conflicts:
            raise ValidationError(
                "extras conflict with PublicationMetadata fields: "
                + ", ".join(conflicts)
            )

        _ = self._package_name()

    def to_payload(self) -> dict[str, Any]:
        """Serialize metadata to a CKAN package action payload."""
        self.validate()

        payload: dict[str, Any] = {
            "name": self._package_name(),
            "title": self.metadata.title,
            "notes": self.metadata.description,
            "tags": [{"name": keyword} for keyword in self.metadata.keywords],
        }

        _add_if_present(payload, "owner_org", self.owner_org)
        if self.private is not None:
            payload["private"] = self.private
        _add_if_present(payload, "license_id", self.metadata.license)
        _add_if_present(payload, "version", self.metadata.version)
        _add_if_present(payload, "type", self.dataset_type)

        if self.groups:
            payload["groups"] = [
                {"name": _required_string(group, "group")} for group in self.groups
            ]

        extras = self._serialized_extras()
        if extras:
            payload["extras"] = [
                {"key": key, "value": value} for key, value in extras.items()
            ]

        return payload

    def _package_name(self) -> str:
        if self.name is not None:
            return _required_string(self.name, "name")

        name = _slugify(self.metadata.title)
        if not name:
            raise ValidationError(
                "name must be provided when title cannot produce a CKAN package name"
            )
        return name

    def _serialized_extras(self) -> dict[str, str]:
        extras = {
            _required_string(key, "extra key"): value
            for key, value in self.extras.items()
        }

        if self.metadata.publication_date is not None:
            extras["publication_date"] = self.metadata.publication_date.isoformat()
        if self.metadata.doi is not None:
            extras["doi"] = self.metadata.doi
        if self.metadata.language is not None:
            extras["language"] = self.metadata.language

        extras["creators"] = _json_value(
            [_person_dict(person) for person in self.metadata.creators]
        )
        if self.metadata.contributors:
            extras["contributors"] = _json_value(
                [_contributor_dict(item) for item in self.metadata.contributors]
            )
        if self.metadata.related_identifiers:
            extras["related_identifiers"] = _json_value(
                [
                    _related_identifier_dict(item)
                    for item in self.metadata.related_identifiers
                ]
            )

        return extras


def _generated_extra_keys(metadata: PublicationMetadata) -> set[str]:
    keys = {"creators"}
    if metadata.publication_date is not None:
        keys.add("publication_date")
    if metadata.doi is not None:
        keys.add("doi")
    if metadata.language is not None:
        keys.add("language")
    if metadata.contributors:
        keys.add("contributors")
    if metadata.related_identifiers:
        keys.add("related_identifiers")
    return keys


def _person_dict(person: Person) -> dict[str, str]:
    data: dict[str, str] = {}
    _add_if_present(data, "family_name", person.family_name)
    _add_if_present(data, "given_names", person.given_names)
    _add_if_present(data, "name", person.name)
    _add_if_present(data, "affiliation", person.affiliation)
    _add_if_present(data, "orcid", person.orcid)
    _add_if_present(data, "gnd", person.gnd)
    return data


def _contributor_dict(contributor: Contributor) -> dict[str, Any]:
    data: dict[str, Any] = {"person": _person_dict(contributor.person)}
    _add_if_present(data, "role", contributor.role)
    return data


def _related_identifier_dict(
    related_identifier: RelatedIdentifier,
) -> dict[str, str]:
    data = {
        "identifier": related_identifier.identifier,
        "relation": related_identifier.relation,
    }
    _add_if_present(data, "resource_type", related_identifier.resource_type)
    return data


def _json_value(value: object) -> str:
    return json.dumps(value, ensure_ascii=True, separators=(",", ":"), sort_keys=True)


def _slugify(value: str) -> str:
    ascii_value = (
        unicodedata.normalize("NFKD", value).encode("ascii", "ignore").decode("ascii")
    )
    slug = re.sub(r"[^a-z0-9]+", "-", ascii_value.lower())
    return slug.strip("-")


def _required_string(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValidationError(f"{field_name} must be a non-empty string")
    return value.strip()


def _add_if_present(data: dict[str, Any], key: str, value: str | None) -> None:
    if value is not None and value.strip():
        data[key] = value.strip()
