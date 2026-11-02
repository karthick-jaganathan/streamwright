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
Rule tables for job manifests. One `Field` table drives both the structural checks (`check_field`) and the JSON
schema (`json_schema_of`), so the schema an editor or an LLM sees is the contract the checker enforces.
"""

import re
from types import MappingProxyType

__all__ = ["Field", "check_field", "json_schema_of", "path_text"]

_JSON_TYPES = MappingProxyType({"string": str, "integer": int, "number": (int, float), "boolean": bool, "object": dict,
                                "array": list})
_TYPE_NAMES = MappingProxyType({"string": "text", "integer": "a whole number", "number": "a number",
                                "boolean": "true or false", "object": "a mapping", "array": "a list"})


class Field:
    """
    One value's rule.
      kind         JSON type name, or a tuple of them (None: any value)
      required     the key must be present (in its parent mapping)
      enum         allowed values;  pattern: regex for text;  minimum: for numbers;  min_items: for lists/mappings
      fields       {key: Field} of a mapping with known keys (unknown keys are errors unless open)
      values       Field of every value of a mapping keyed by names (connections, spec.config)
      items        Field of every list item
      variants     {discriminator value: {key: Field}}: extra keys allowed when `discriminator` has that value
    """

    def __init__(self, kind=None, *, required=False, enum=None, pattern=None, minimum=None, min_items=None,
                 fields=None, values=None, items=None, open=False, description="", discriminator=None,
                 variants=None):
        self.kind = kind if kind is None or isinstance(kind, tuple) else (kind,)
        self.required, self.enum, self.pattern, self.minimum, self.min_items = (
            required, enum, pattern, minimum, min_items)
        self.fields, self.values, self.items, self.open = fields, values, items, open
        self.description, self.discriminator, self.variants = description, discriminator, variants or {}


def path_text(path):
    text = ""
    for part in path:
        text += "[%d]" % part if isinstance(part, int) else ("." if text else "") + str(part)
    return text


def _is_kind(value, kind):
    expected = _JSON_TYPES[kind]
    return isinstance(value, expected) and not (kind in ("integer", "number") and isinstance(value, bool))


def check_field(value, field, path=()):
    """[(code, path tuple, message)] for `value` against `field`."""
    if field.kind is not None and not any(_is_kind(value, kind) for kind in field.kind):
        return [("bad-type", path, "expected %s" % " or ".join(_TYPE_NAMES[k] for k in field.kind))]
    found = []
    if field.enum is not None and value not in field.enum:
        found.append(("bad-value", path, "%r is not one of %s" % (value, ", ".join(map(str, field.enum)))))
    if field.pattern is not None and isinstance(value, str) and not re.match(field.pattern, value):
        found.append(("bad-value", path, "%r does not match %s" % (value, field.pattern)))
    if field.minimum is not None and isinstance(value, (int, float)) and value < field.minimum:
        found.append(("bad-value", path, "must be at least %s" % field.minimum))
    if field.min_items is not None and isinstance(value, (list, dict)) and len(value) < field.min_items:
        found.append(("bad-value", path, "needs at least %d entr%s" % (
            field.min_items, "y" if field.min_items == 1 else "ies")))
    if isinstance(value, dict):
        found.extend(_check_mapping(value, field, path))
    if isinstance(value, list) and field.items is not None:
        for index, item in enumerate(value):
            found.extend(check_field(item, field.items, (*path, index)))
    return found


def _variant(value, field):
    """The extra fields of the variant `value` selects, or None."""
    if field.discriminator is None:
        return None
    selector = value.get(field.discriminator)
    return field.variants.get(selector) if isinstance(selector, str) else None


def _known_fields(value, field):
    return dict(field.fields or {}, **(_variant(value, field) or {}))


def _check_mapping(value, field, path):
    found = []
    if field.fields is not None:
        known = _known_fields(value, field)
        for key, rule in known.items():
            if rule.required and key not in value:
                found.append(("missing-key", (*path, key), "`%s` is required" % key))
        for key, item in value.items():
            if key in known:
                found.extend(check_field(item, known[key], (*path, key)))
            elif not field.open or _variant(value, field) is not None:
                found.append(("unknown-key", (*path, key), "unknown key %r (keys: %s)" % (
                    key, ", ".join(sorted(known)))))
    if field.values is not None:
        for key, item in value.items():
            found.extend(check_field(item, field.values, (*path, key)))
    return found


def json_schema_of(field):
    """The JSON schema (draft 7) of a Field."""
    schema = {}
    if field.kind is not None:
        schema["type"] = list(field.kind) if len(field.kind) > 1 else field.kind[0]
    if field.description:
        schema["description"] = field.description
    if field.enum is not None:
        schema["enum"] = list(field.enum)
    if field.pattern is not None:
        schema["pattern"] = field.pattern
    if field.minimum is not None:
        schema["minimum"] = field.minimum
    if field.min_items is not None:
        schema["minItems" if field.kind == ("array",) else "minProperties"] = field.min_items
    if field.items is not None:
        schema["items"] = json_schema_of(field.items)
    if field.values is not None:
        schema["additionalProperties"] = json_schema_of(field.values)
    if field.fields is not None:
        schema["properties"] = {k: json_schema_of(v) for k, v in field.fields.items()}
        required = sorted(k for k, v in field.fields.items() if v.required)
        if required:
            schema["required"] = required
        if field.variants:
            schema.update(_variants_schema(field))
        elif not field.open:
            schema["additionalProperties"] = False
    return schema


def _variants_schema(field):
    """Known keys plus, per discriminator value, the keys of that variant (if/then); other values stay open."""
    branches = []
    for value, extra in sorted(field.variants.items()):
        properties = {k: json_schema_of(v) for k, v in dict(field.fields, **extra).items()}
        then = {"properties": properties, "additionalProperties": False}
        required = sorted(k for k, v in extra.items() if v.required)
        if required:
            then["required"] = required
        branches.append({"if": {"properties": {field.discriminator: {"const": value}},
                                "required": [field.discriminator]}, "then": then})
    return {"allOf": branches} if branches else {}
