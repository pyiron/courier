import importlib.metadata

try:
    # Installed package will find its version
    __version__ = importlib.metadata.version(__name__)
except importlib.metadata.PackageNotFoundError:
    # Repository clones will register an unknown version
    __version__ = "0.0.0+unknown"

from courier.http_client import HttpClient
from courier.ontodocker import (
    create_empty_dataset,
    delete_dataset,
    download_dataset_as_turtle_file,
    extract_dataset_names,
    get_all_dataset_sparql_endpoints,
    rectify_endpoints,
    upload_turtlefile,
)

__all__ = [
    "HttpClient",
    "rectify_endpoints",
    "get_all_dataset_sparql_endpoints",
    "extract_dataset_names",
    "download_dataset_as_turtle_file",
    "create_empty_dataset",
    "upload_turtlefile",
    "delete_dataset",
]
