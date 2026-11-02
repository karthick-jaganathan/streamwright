"""
The shared operation catalog: the operations a connector family offers, declared once with the union of the
arguments every engine knows and ONE vocabulary across engines (e.g. a write `mode` is `error`, `overwrite` or
`append` wherever it runs). A connector's spec lists the catalog operations it offers - `operations=(files.READ,
files.WRITE)` - and adds an argument of its own with `Operation.with_arguments`. Which of the arguments (and choices)
an engine supports is not declared here: each engine declares it once, as EngineCapabilities in its io module
(`streamwright.core.io.files`, `streamwright.spark.io.files`), and the generic argument check applies it.

Modules, one per family: `files` (object storage reads and writes); later `databases` and `messages`.
"""
