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

from courier._version import __version__
from courier.services.ontodocker import OntodockerClient

__all__ = ["__version__", "OntodockerClient"]
