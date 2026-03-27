# courier

`courier` is a Python client library for publishing and querying semantic assets through remote HTTP services.

The broader goal is to support publication of workflow recipes, workflow instances, and related data products. The concrete implementation in this repository currently focuses on Ontodocker-backed RDF datasets and SPARQL endpoints. Implementations to support CKAN instances and Zenodo are coming soon.

In practice, `courier` solves a narrower and more immediate problem: service APIs tend to require repetitive URL assembly, token handling, request configuration, response parsing, and occasional tolerance for historical eccentricities. `courier` turns that work into explicit Python clients with a small public surface.

`courier` is not a workflow engine, scheduler, or triple store. It is the transport layer with better manners.

## What `courier` currently provides

The public API currently exposes two main entry points:

- `HttpClient` for direct HTTP(S) access with normalized base URLs, bearer-token handling, timeouts, TLS verification, and consistent error reporting.
- `OntodockerClient` for service-specific access to Ontodocker resources.

`OntodockerClient` currently supports:

- discovery of dataset SPARQL endpoints
- listing available datasets
- creating and deleting datasets
- downloading datasets as Turtle
- uploading Turtle files
- uploading `rdflib.Graph` objects
- executing SPARQL queries and returning either raw text or `pandas.DataFrame` results

A legacy functional API remains available in `courier.ontodocker` for backward compatibility. New code should use `OntodockerClient`.

## Project structure

The repository is organized in layers.

- `courier/http_client.py` contains the reusable HTTP client and shared request configuration.
- `courier/transport/` contains small transport helpers for authentication headers, session creation, URL normalization, and response handling.
- `courier/services/ontodocker/` contains the Ontodocker client and its resource modules for endpoints, datasets, SPARQL, and small data models.

The intent is straightforward: generic transport code stays generic, service-specific behavior lives under `courier/services`.

## Getting started

`courier` supports Python 3.11 to 3.13. Development in this repository currently targets Python 3.12.

For local development, a minimal setup using `conda` is:

```bash
git clone https://github.com/pyiron/courier.git
cd courier
conda create -n courier python=3.12
conda activate courier
pip install -e .
```

If you want the documentation environment as well, see `docs/environment.yml`.
A release on Pypi and conda-forge is coming soon.

The main service client is `OntodockerClient`. The `address` argument may be either `host[:port]` or a full URL. If no scheme is provided, `https` is assumed.

```python
from courier import OntodockerClient

client = OntodockerClient(
    "ontodocker.example.org",
    token="your-token",
)

datasets = client.datasets.list()
print(datasets)

query = """
SELECT ?s ?p ?o
WHERE {
  ?s ?p ?o
}
LIMIT 5
"""

df = client.sparql.query_df(
    "example_dataset",
    query,
    columns=["s", "p", "o"],
)

print(df)
```

Common dataset operations are exposed explicitly on `client.datasets`:

```python
client.datasets.create("example_dataset")
client.datasets.upload_turtlefile("example_dataset", "graph.ttl")
ttl = client.datasets.fetch_turtle("example_dataset")
client.datasets.delete("example_dataset")
```

If you need low-level access or want to build another service adapter, use `HttpClient` directly and keep the service-specific wiring in `courier/services`.
