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
The shared operation catalog: the operations a connector family offers, declared once with the union of the
arguments every engine knows and ONE vocabulary across engines (e.g. a write `mode` is `error`, `overwrite` or
`append` wherever it runs). A connector's spec lists the catalog operations it offers - `operations=(files.READ,
files.WRITE)` - and adds an argument of its own with `Operation.with_arguments`. Which of the arguments (and choices)
an engine supports is not declared here: each engine declares it once, as EngineCapabilities in its io module
(`streamwright.core.io.files`, `streamwright.spark.io.files`), and the generic argument check applies it.

Modules, one per family: `files` (object storage reads and writes); later `databases` and `messages`.
"""
