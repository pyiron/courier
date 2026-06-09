# Metadata Design

## Scope

Courier separates **publication metadata authoring** from **service payload construction**. The common model describes publication concepts; service adapters apply platform-specific fields, defaults, validation, and wire formats.

The Zenodo path is part of the current public API. The DataPortal path described here is implemented on the pending `feature/dataportal/*` stack and should be treated as provisional until it has been validated against the deployed PMD schema.

```text
PublicationMetadata
        |
        +--> ZenodoMetadata ----> Zenodo deposition payload
        |
        +--> DataportalMetadata -> CKAN package payload
```

This is composition, not inheritance. `courier.metadata` has no service imports, and the adapters depend on the common model rather than the reverse. Neutral types are re-exported from `courier`; adapters remain in their `courier.services.<service>` namespaces so service-specific semantics stay visible.

## Service-neutral authoring model

`PublicationMetadata` and its nested `Person`, `Contributor`, and `RelatedIdentifier` models live in `courier.metadata`. They cover concepts that have useful meaning across repositories:

| Model | Responsibility |
|---|---|
| `PublicationMetadata` | title, description, date, creators, contributors, keywords, license, DOI, version, language, related identifiers |
| `Person` | human-readable or structured name plus affiliation, ORCID, and GND |
| `Contributor` | a person plus an optional service-neutral role |
| `RelatedIdentifier` | an identifier, relation, and optional resource type |

The common model uses Pydantic dataclasses. Construction and assignment strip text, coerce ISO date strings and nested mappings, and reject empty required fields. Collection inputs become tuples:

```python
from courier import Person, PublicationMetadata

publication = PublicationMetadata(
    title="Steel heat-treatment data",
    description="Measurements and processing parameters.",
    publication_date="2026-06-08",
    creators=[{"family_name": "Doe", "given_names": "Jane"}],
    keywords=["steel", "heat treatment"],
    license="CC-BY-4.0",
)

assert publication.publication_date.isoformat() == "2026-06-08"
assert publication.creators == (Person(family_name="Doe", given_names="Jane"),)
```

Tuples prevent unvalidated list mutation, while assignment validation allows controlled replacement. This is not deep immutability: nested `Person` objects remain mutable, although their own assignments are validated.

### How far Pydantic is leveraged

Courier uses Pydantic dataclasses selectively in the service-neutral layer:

| Capability | Current use |
|---|---|
| Type-driven parsing | ISO date strings, nested dictionaries, and input lists are converted to the annotated `date`, nested dataclass, and tuple types |
| Field validators | required text is stripped and checked; blank optional text becomes `None`; keywords are normalized |
| Model validators | cross-field invariants require a usable person identity and at least one creator |
| Assignment validation | `ConfigDict(validate_assignment=True)` reapplies coercion and validation when an attribute is replaced |
| Standard dataclass API | constructors, equality, representation, and `dataclasses.fields()` remain available without exposing a `BaseModel` API |

This captures the main benefit needed by the authoring model: one typed validation path for both initial input and later assignment, including nested objects. Combined with tuple collection fields, it closes the most obvious route for bypassing validation through in-place list mutation.

The broader Pydantic feature set is not currently used. Courier does not expose JSON Schema through `TypeAdapter`, use Pydantic serialization as a service wire format, configure strict input mode, add field aliases for service names, or generate adapters from model schemas. `ZenodoMetadata` and `DataportalMetadata` are standard dataclasses with manual validation and serialization. This is intentional: their payload rules are service semantics, not merely alternate names for common fields.

The result is substantial but incomplete leverage. Pydantic removes most generic coercion and nested-model validation from `courier.metadata`, while service adapters still contain hand-written validation and conversion. That keeps the dependency localized and payload generation explicit, but forfeits a uniform error type, automatic schemas for adapters, and a common serialization framework. Moving the adapters to Pydantic would only be justified if those benefits outweigh compatibility work around existing constructors, mutable helper methods, payload methods, and `courier.exceptions.ValidationError`.

The common layer deliberately validates stable structure, not external vocabularies. For example, it requires a non-empty DOI or ORCID string but does not prove that the identifier is syntactically valid or registered. Likewise, contributor roles and related-identifier relations remain open strings because their accepted vocabularies differ by service.

`publication_date` has no default. Assigning "today" in the neutral model would conflate authoring, upload, release, and publication dates. A service adapter may still apply a service-specific default.

## Explicit adapter boundary

Client operations do not accept `PublicationMetadata` directly. Callers must select an adapter:

```python
from courier.services.zenodo import ZenodoMetadata

zenodo = ZenodoMetadata.software(publication)
draft = client.depositions.create(zenodo)
```

The explicit wrapper is useful because the common object does not contain enough information to construct every service payload. Zenodo needs an upload type and access policy; CKAN needs a package name and may need an owning organization. Implicit adaptation would require hidden defaults or an expanding set of operation arguments.

Serialization occurs immediately before the service resource delegates to the transport layer. Metadata is therefore operation-scoped rather than stored on a client, which keeps clients reusable and avoids stale mutable state.

Both services also accept raw mappings as an escape hatch for fields Courier does not model. This preserves API coverage, but intentionally bypasses local metadata validation:

```python
raw_zenodo = zenodo.to_api_dict()
raw_zenodo["references"] = ["DOI:10.0000/example"]
client.depositions.set_metadata(draft, raw_zenodo)

client.datasets.patch(dataset, {"private": True})
```

## Zenodo adapter

`ZenodoMetadata` owns Zenodo concepts such as upload and publication types, access rights, embargoes, communities, grants, DOI reservation, and the `{"metadata": ...}` envelope. Common fields are read from the composed `PublicationMetadata` and converted into Zenodo's names and nested structures.

```python
from courier.services.zenodo import ZenodoMetadata

zenodo = ZenodoMetadata.dataset(publication)
zenodo.notes = "Processed and quality-controlled measurements."
zenodo.add_community("materials-science")

draft = client.depositions.create(zenodo)
```

The current mapping is:

| Common field | Zenodo metadata representation |
|---|---|
| `title` | `title` |
| `description` | `description` |
| `publication_date` | ISO date in `publication_date`; defaults to the current date when absent |
| `creators` | `creators`, with structured names rendered as `"family_name, given_names"` |
| `contributors` | `contributors`, with `role` mapped to Zenodo's required contributor `type` |
| `keywords` | `keywords` |
| `license` | `license` |
| `doi` | `doi` |
| `version` | `version` |
| `language` | `language`; the adapter defaults to `"eng"` when the common value is absent |
| `related_identifiers` | `related_identifiers`, preserving identifier, relation, and optional resource type |

Zenodo-only fields remain on the adapter: `upload_type`, publication or image subtype, access policy, embargo and restricted-access information, DOI pre-reservation, notes, communities, and grants. Convenience constructors such as `dataset()`, `software()`, `publication()`, and `image()` make the required output type explicit.

For the example above, the payload is:

```python
{
    "metadata": {
        "upload_type": "dataset",
        "publication_date": "2026-06-08",
        "title": "Steel heat-treatment data",
        "creators": [{"name": "Doe, Jane"}],
        "description": "Measurements and processing parameters.",
        "access_right": "open",
        "license": "CC-BY-4.0",
        "keywords": ["steel", "heat treatment"],
        "notes": "Processed and quality-controlled measurements.",
        "communities": [{"identifier": "materials-science"}],
        "language": "eng",
    }
}
```

Adapter validation is stricter where Zenodo requires it. An open or embargoed record requires a license, and a common `Contributor` must have a role because Zenodo serializes it as the contributor `type`. If the common publication date is absent, the legacy adapter defaults it to the current date at serialization time. If language is absent, it retains the Zenodo adapter's `"eng"` default. These are service semantics, not properties of the neutral model.

For backward compatibility, `ZenodoMetadata.metadata` is optional and the older direct fields still exist. Their use is deprecated. When a `PublicationMetadata` is supplied, setting the same common fields directly on `ZenodoMetadata` is rejected to avoid two sources of truth.

## DataPortal adapter

The pending `DataportalMetadata` starts with the clean boundary that Zenodo is migrating toward: `metadata: PublicationMetadata` is required, and the adapter contains only CKAN/DataPortal fields:

```python
from courier.services.dataportal import DataportalMetadata

dataportal = DataportalMetadata(
    metadata=publication,
    owner_org="materials-org",
    private=False,
    groups=["heat-treatment"],
    extras={"pmd_profile": "dataset-v1"},
    dataset_type="dataset",
)

dataset = client.datasets.create(dataportal)
```

The lower-level `courier.services.ckan` package handles action calls and CKAN response models, but not publication mapping. Keeping PMD profile and DCAT assumptions in the DataPortal layer allows the CKAN substrate to remain small and reusable without claiming that every CKAN deployment shares this schema.

The current mapping is:

| Common field | CKAN package representation |
|---|---|
| `title` | `title` |
| `description` | `notes` |
| `keywords` | `tags=[{"name": ...}]` |
| `license` | `license_id` |
| `version` | `version` |
| `publication_date`, `doi`, `language` | string-valued CKAN extras |
| `creators`, `contributors`, `related_identifiers` | deterministic JSON stored in string-valued extras |

DataPortal-only fields map to `name`, `owner_org`, `private`, `groups`, `type`, and additional extras. If `name` is omitted, the adapter derives an ASCII, lower-case slug from the title. It rejects explicit extras that would overwrite generated common fields.

For the example above, the relevant payload shape is:

```python
{
    "name": "steel-heat-treatment-data",
    "title": "Steel heat-treatment data",
    "notes": "Measurements and processing parameters.",
    "tags": [{"name": "steel"}, {"name": "heat treatment"}],
    "license_id": "CC-BY-4.0",
    "owner_org": "materials-org",
    "private": False,
    "type": "dataset",
    "groups": [{"name": "heat-treatment"}],
    "extras": [
        {"key": "pmd_profile", "value": "dataset-v1"},
        {"key": "publication_date", "value": "2026-06-08"},
        {
            "key": "creators",
            "value": '[{"family_name":"Doe","given_names":"Jane"}]',
        },
    ],
}
```

The adapter serializes structured extras with sorted keys and compact JSON, so payloads are deterministic. This is practical for a generic CKAN backend, but it is the least settled part of the design. JSON-valued extras are opaque to ordinary CKAN filtering, schema validation, and DCAT projection unless PMD plugins interpret them. Their exact keys and shapes should not be considered a stable metadata profile until checked against the deployed PMD extensions.

Dataset creation and patching accept either `DataportalMetadata` or a raw mapping. A typed adapter always emits a complete authoring payload, including the derived name, title, tags, and generated extras. Consequently, genuinely partial updates currently require a raw mapping. `package_patch` prevents CKAN's full replacement behavior, but it does not make a complete typed payload semantically partial.

## Ontology compatibility

Courier is conceptually close to [NFDIcore 3.0.5](https://ise-fizkarlsruhe.github.io/nfdicore/3.0.5/): its common fields correspond to NFDIcore concepts for data collections and publications, people and organizations, creator and contributor roles/processes, licences, version numbers, and registered identifiers such as DOI, ORCID, and GND. This makes a future RDF adapter plausible, but the present schema is not formally NFDIcore-compatible: Courier stores flat values and nested records without ontology class IRIs, identifier individuals, role realizations, creation/contribution processes, or explicit graph relations. `ZenodoMetadata` preserves roughly the same bibliographic overlap but translates it into Zenodo vocabulary and JSON rather than NFDIcore semantics; communities, grants, access rules, and free-string relation types would need explicit mappings. Compatibility with [PMDco 3.0.0](https://materialdigital.github.io/core-ontology/) is weaker at the metadata-record level and stronger at the asset level. PMDco models data sets, data items, files, identifiers, publications, and URLs, but its main value is the semantic representation of material entities, process chains, inputs and outputs, measurements, qualities, and what information is about. Courier's generic and service metadata do not express those structures. The DataPortal client can upload PMDco RDF and query its SPARQL endpoint, so a dataset asset may be fully PMDco-aligned, but `DataportalMetadata` currently stores publication structures as JSON-valued CKAN extras and does not link the CKAN package, its creators, or its assets to NFDIcore or PMDco entities. Therefore the current layers are schema-mappable, not ontology-interoperable. Proper interoperability should be implemented as an explicit RDF/DCAT profile or adapter that assigns stable IRIs and relations while leaving `PublicationMetadata` service- and ontology-neutral.

## Authoring and response models

Courier intentionally distinguishes outbound authoring models from inbound service responses:

- `PublicationMetadata` plus an adapter constructs requests.
- `DepositionInfo.metadata` keeps Zenodo response metadata as a raw dictionary.
- `DataportalDatasetInfo` exposes selected stable CKAN fields and preserves the complete package response in `raw`.

This avoids pretending that server-normalized responses are lossless instances of Courier's authoring schema. It also means there is no common read-modify-write round trip. Callers must inspect raw response data and construct a new adapter or send a raw patch.

## Assessment

The design has a sound dependency direction and makes service semantics visible at call sites. It avoids a lowest-common-denominator payload model, keeps remote I/O out of validation, and postpones an abstract adapter hierarchy until more services demonstrate a genuinely common interface.

The main limitations are:

1. Common Pydantic models raise `pydantic.ValidationError`, while adapters raise `courier.exceptions.ValidationError`. Callers do not have one validation exception contract.
2. Raw mappings are necessary for forward compatibility but create a sharp drop from typed guarantees to no local guarantees.
3. Identifier and vocabulary checks are mostly delegated to services. This is appropriate for changing remote vocabularies, but optional, explicit remote validation may eventually be useful.
4. DataPortal's structured extras and slug rules are provisional. The slug is normalized but not checked against all CKAN naming constraints, length limits, or collisions.
5. The DataPortal typed model is suitable for create/full authoring, not partial patching or response round trips.

The immediate priority should be deployment validation of the PMD field profile and DCAT projection. A typed partial-update model or response parser should be added only when a concrete workflow requires it; neither belongs in `PublicationMetadata`. The explicit adapter boundary should remain even if small serialization helpers are later shared.
