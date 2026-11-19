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
`streamwright serve` and `streamwright worker` (plan §5): the job CLI over HTTP. `serve` authenticates a caller,
checks the request as `--set` / `--input` are checked and queues the run in the state store (sw_requests); workers
claim queued runs and run them with the same engine as `streamwright duckdb run`. The jobs a service exposes, who may
call them and what callers may set are in its serve.yaml (`config`). Only the HTTP app (`app`, `auth`) needs the
`serve` extra: pip install "streamwright[serve]".
"""
