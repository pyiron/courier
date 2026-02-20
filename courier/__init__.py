"""
courier: Interfaces for publishing workflow recipes, instances and related assets.

Public API is intentionally small; import service clients from `courier.services`.
"""

import importlib.metadata

try:
    # Installed package will find its version
    __version__ = importlib.metadata.version(__name__)
except importlib.metadata.PackageNotFoundError:
    # Repository clones will register an unknown version
    __version__ = "0.0.0+unknown"

from courier.ontodocker import (
    rectify_endpoints,
    get_all_dataset_sparql_endpoints,
    extract_dataset_names,
    download_dataset_as_turtle_file,
    create_empty_dataset,
    upload_turtlefile,
    delete_dataset,
)

__all__ = [
    "rectify_endpoints",
    "get_all_dataset_sparql_endpoints",
    "extract_dataset_names",
    "download_dataset_as_turtle_file",
    "create_empty_dataset",
    "upload_turtlefile",
    "delete_dataset",
]
