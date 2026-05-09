"""Identity module — orgs, users, memberships, API keys, JWT auth.

Public contract: import ``service`` for cross-module business operations.
Other modules MUST NOT import ``models`` or ``repository`` directly — those are
the module's private surface.
"""

from app.modules.identity import service  # noqa: F401  re-exported for cross-module use
