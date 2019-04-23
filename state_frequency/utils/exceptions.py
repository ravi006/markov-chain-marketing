"""Customer State common exception definitions."""

from __future__ import absolute_import


class StateConfigError(Exception):
    """When Failed to get correct config parameters."""

    pass


class StateConnectionError(Exception):
    """Use when unable to establish connection."""

    pass


class StateParameterError(Exception):
    """Use when mandatory parameter is missing or value is incorrect."""

    pass


class StateFileError(Exception):
    """Use when File Reading/ Writing fail."""

    pass