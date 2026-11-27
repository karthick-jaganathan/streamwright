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

"""The result of a DuckDBJob run, and of a runtime.fan_out run (one entry per item in `passes`)."""

from dataclasses import dataclass, field

__all__ = ["RunResult", "apply_counts", "combine_passes", "describe_output", "output_counts"]


@dataclass
class RunResult:
    exit_code: int
    job_name: str
    plan: str
    batches: list[str] = field(default_factory=list)
    outputs: dict[str, int | None] = field(default_factory=dict)      # output -> rows, from the receipts
    skipped: set[str] = field(default_factory=set)                    # outputs already committed in an earlier attempt
    error: str | None = None
    # output -> where it wrote: its files (a file_name template's, split_by's), else its destination
    locations: dict[str, list[str]] = field(default_factory=dict)
    lock_skipped: bool = False                                        # the lock policy pruned all the run's work
    lease_held: bool = False                                          # stopped: another run holds the job's lease
    passes: list[dict] = field(default_factory=list)                  # runtime.fan_out: one entry per item
    fan_out: dict | None = None                                       # runtime.fan_out's over and as
    rejected: dict[str, int] = field(default_factory=dict)            # apply output -> rows the vendor rejected
    # the apply outputs: their `outputs` count is the rows applied (not written), `rejected` the rows refused
    apply_outputs: set[str] = field(default_factory=set)


def output_counts(name, rows, rejected, apply_outputs):
    """
    What an output did, for a summary: a table or file output's {rows} written; an apply output's {rows, applied,
    rejected} (rows: applied, kept for the readers that know only `rows`).
    """
    if name in apply_outputs:
        return {"rows": rows, "applied": rows, "rejected": rejected.get(name, 0)}
    return {"rows": rows, **({"rejected": rejected[name]} if rejected.get(name) else {})}


def describe_output(name, rows, rejected, apply_outputs):
    """An output's part of a run's recap line: `orders 120 rows`, `upload 2 applied, 2 rejected`."""
    if name in apply_outputs:
        return "%s %s applied%s" % (name, "?" if rows is None else rows,
                                    ", %d rejected" % rejected[name] if rejected.get(name) else "")
    return "%s %s rows" % (name, "?" if rows is None else rows)


def apply_counts(counts):
    """An applier's counts as (rows applied per apply output, rows rejected per apply output that had any)."""
    return ({name: count["applied"] for name, count in counts.items()},
            {name: count["failed"] for name, count in counts.items() if count["failed"]})


def combine_passes(job_name, outcomes, fan_out, redact):
    """One RunResult for the passes: every batch, the outputs' rows summed, and a `passes` entry per item."""
    total = RunResult(0, job_name, "", fan_out={"over": fan_out["over"], "as": fan_out["as"]})
    for outcome in outcomes:
        result = outcome.result
        entry = {"item": outcome.label, "status": outcome.status}
        if result is not None:
            total.plan = total.plan or result.plan
            total.batches.extend(result.batches)
            total.skipped.update(result.skipped)
            total.apply_outputs.update(result.apply_outputs)
            for name, rows in result.rejected.items():
                total.rejected[name] = total.rejected.get(name, 0) + rows
            for name, rows in result.outputs.items():
                total.outputs[name] = None if rows is None or total.outputs.get(name, 0) is None else \
                    total.outputs.get(name, 0) + rows
                total.locations.setdefault(name, []).extend(result.locations.get(name, ()))
            entry.update(status="skipped" if result.lock_skipped and not result.exit_code else outcome.status,
                         job_ids=list(result.batches), outputs=dict(result.outputs),
                         **({"rejected": dict(result.rejected)} if result.rejected else {}))
        error = redact("%s: %s" % (type(outcome.error).__name__, outcome.error)) if outcome.error is not None \
            else result.error if result is not None and result.exit_code else None
        if error:
            entry["error"] = error
        total.passes.append(entry)
    failed = [entry["item"] for entry in total.passes if entry["status"] in ("failed", "not_run")]
    if failed:
        total.exit_code = 1
        total.error = "%d of %d %s(s) failed or did not run: %s" % (len(failed), len(total.passes), fan_out["as"],
                                                                     ", ".join(failed))
    total.lock_skipped = bool(total.passes) and all(entry["status"] == "skipped" for entry in total.passes)
    stopped = [outcome for outcome in outcomes if outcome.error is not None
               or (outcome.result is not None and outcome.result.exit_code)]
    total.lease_held = bool(stopped) and all(outcome.result is not None and outcome.result.lease_held
                                             for outcome in stopped)
    return total
