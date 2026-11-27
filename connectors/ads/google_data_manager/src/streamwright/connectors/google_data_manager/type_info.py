#!/usr/bin/env python
# /*************************************************************************
# * Copyright 2026 Karthick Jaganathan
# *
# * Licensed under the Apache License, Version 2.0 (the "License");
# * you may not use this file except in compliance with the License.
# * You may obtain a copy of the License at
# *
# * https://www.apache.org/licenses/LICENSE-2.0
# *
# * Unless required by applicable law or agreed to in writing, software
# * distributed under the License is distributed on an "AS IS" BASIS,
# * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# * See the License for the specific language governing permissions and
# * limitations under the License.
# **************************************************************************/
"""
Data Manager types from the installed google-ads-datamanager library, offline: a message's fields and their types
(e.g. Event, AudienceMember, UserIdentifier) or an enum's values (e.g. Encoding, ConsentStatus, or a nested one such
as ProductAccount.AccountType).
"""

import enum
from typing import Any

from google.ads import datamanager_v1 as library
from proto.primitives import ProtoType

from streamwright.connectors.google_data_manager.surface import sdk_version

__all__ = ["describe_type"]


def _field_type(field: Any) -> str:
    if field.enum is not None:
        text = field.enum.__qualname__
    elif field.message is not None:
        text = getattr(field.message, "__qualname__", field.message.__name__)
    else:
        text = ProtoType(field.proto_type).name.lower()
    return "list<%s>" % text if field.repeated else text


def describe_type(name: str) -> dict[str, Any] | None:
    """A message (`Event`) or an enum (`Encoding`, `ProductAccount.AccountType`) of the installed library."""
    found: Any = library
    for part in name.split("."):
        found = getattr(found, part, None) if part and not part.startswith("_") else None
        if found is None:
            return None
    if isinstance(found, type) and issubclass(found, enum.Enum):
        return {"kind": "enum", "library": sdk_version(), "values": [member.name for member in found]}
    if hasattr(found, "meta") and hasattr(found.meta, "fields"):
        return {"kind": "object", "library": sdk_version(),
                "fields": {field: _field_type(spec) for field, spec in found.meta.fields.items()}}
    return None
