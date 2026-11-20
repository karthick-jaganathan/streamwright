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
Google Ads types from the installed google-ads library (its default API version), offline: an enum's values (e.g.
CampaignStatus or CampaignStatusEnum) or a resource's fields and their types (e.g. Campaign, AdGroup).
"""

import importlib
from typing import Any

from proto.primitives import ProtoType

from streamwright.connectors.google_ads.surface import _snake, default_version

__all__ = ["describe_type"]


def _module(kind: str, name: str) -> Any:
    try:
        return importlib.import_module("google.ads.googleads.%s.%s.types.%s" % (default_version(), kind, _snake(name)))
    except ImportError:
        return None


def _field_type(field: Any) -> str:
    if field.enum is not None:
        text = field.enum.__qualname__
    elif field.message is not None:
        text = field.message.__name__
    else:
        text = ProtoType(field.proto_type).name.lower()
    return "list<%s>" % text if field.repeated else text


def describe_type(name: str) -> dict[str, Any] | None:
    """An enum (`CampaignStatus`, `CampaignStatusEnum`) or a resource (`Campaign`) of the default API version."""
    base = name[:-len("Enum")] if name.endswith("Enum") else name
    module = _module("enums", base)
    wrapper = getattr(module, base + "Enum", None) if module is not None else None
    enum = getattr(wrapper, base, None) if wrapper is not None else None
    if enum is not None:
        return {"kind": "enum", "api_version": default_version(), "values": [member.name for member in enum]}
    module = _module("resources", name)
    resource = getattr(module, name, None) if module is not None else None
    if resource is None or not hasattr(resource, "meta"):
        return None
    return {"kind": "object", "api_version": default_version(),
            "fields": {field: _field_type(spec) for field, spec in resource.meta.fields.items()}}
