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
Marketing API types from the installed facebook_business, offline: an object's fields and their types and its enums
(e.g. AdsInsights), or an enum's values (e.g. AdsInsights.DatePreset, AdsInsights.Level).
"""

from typing import Any

from streamwright.connectors.meta_ads.client import _object_class

__all__ = ["describe_type"]


def _public(cls: type) -> dict[str, Any]:
    return {key: value for key, value in vars(cls).items() if not key.startswith("_")}


def describe_type(name: str) -> dict[str, Any] | None:
    """`Object` (its fields and enums) or `Object.Enum` (its values), or None."""
    owner, _, member = name.partition(".")
    cls = _object_class(owner)
    if cls is None:
        return None
    enums = sorted(key for key, value in _public(cls).items() if isinstance(value, type) and key != "Field")
    if not member:
        types = getattr(cls, "_field_types", {})
        fields = getattr(cls, "Field", None)
        return {"kind": "object", "fields": {field: types.get(field) for field in _public(fields).values()}
                if fields is not None else {}, "enums": ["%s.%s" % (owner, enum) for enum in enums]}
    if member not in enums:
        return None
    return {"kind": "enum", "values": list(_public(getattr(cls, member)).values())}
