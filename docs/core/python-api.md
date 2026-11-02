---
layout: default
title: Python API
parent: streamwright
nav_order: 7
permalink: /core/python-api/
---

# From Python

Validate and run a source from Python instead of the `streamwright` command.

```python
from streamwright.core.engine.runner import SourceRunner
from streamwright.core.outputs.output import SingerOutput
from streamwright.core.validation.engine import validate_source
from streamwright.core.config.loader import load_source

path = "examples/sources/readers/files_demo"          # a source folder, or a source file (needs streamwright-files)
errors = [issue for issue in validate_source(path) if issue.severity == "error"]
assert not errors, errors                             # streamwright run refuses invalid sources the same way
runner = SourceRunner(load_source(path), config={"data_root": path + "/data"}, secrets={},
                      output=SingerOutput())
state = runner.run()                                  # or run(["orders"]) for some streams
```

- **Reuse a compiled plan:** pass `plan=sql.plan_source(source)` to `SourceRunner(...)` when you have already
  planned the source; `run()` does not compile it again.
- **Collect a stream:** `result = runner.collect("orders", state=prior_state)` returns a single-use iterator
  for `(export, record)` pairs. Execution starts when it is iterated; records are produced lazily and buffered
  in a bounded queue. Exhaust the iterator before reading `result.state` (resulting bookmarks) or `result.partial`
  (skipped/inherited partitions). Required parent streams run too; collection does not change `runner.state` or
  `runner.output`. The documented runner API is its constructor, `run()`, `collect()` and `plan`.
- **Diagnostics:** `from streamwright.core.validation import Issue` is the stable type returned by the source
  validator; its attributes include `severity`, `code`, `message`, `file`, `path`, `line` and `column`.
- **Secrets:** pass them as `secrets={...}`. For example, `examples/sources/ads/google_ads` takes `developer_token`,
  `client_id`, `client_secret` and `refresh_token`.
- **More entry points:** the full list (validation, loading, running, outputs) is in the
  [API reference]({{ site.baseurl }}/api-reference/#python).

## Connector-author SDK

Import the connector definition types, query types, secret masking, SQL quoting and `PAGE_SIZE` from
`streamwright.core.sdk` (the contract types also from `streamwright.core.sdk.contract`), rather than private engine
modules:

```python
from streamwright.core.sdk import ConnectorDefinition, Field, Operation, QueryBuilder, Redactor, quote_ident, SDK_VERSION
```

The facade has an explicit `__all__` and a `py.typed` marker. Plug-ins declare a compatible
`StreamWright-SDK-Version` distribution metadata major (currently `1`), or a
`Requires-Dist: streamwright` constraint admitting the installed core version. An incompatible
plug-in is refused before its entry point is imported.
