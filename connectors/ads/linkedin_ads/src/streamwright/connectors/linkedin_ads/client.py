# Copyright 2026 Karthick Jaganathan
# SPDX-License-Identifier: Apache-2.0

"""
What the linkedin_ads connector knows about its vendor API: nothing hardcoded. The source's `auth:` and `http:` blocks
declare the endpoints and token service (contract v1.3 §6b); the source engine's HTTP reader runs its requests.
"""

__all__: list[str] = []
