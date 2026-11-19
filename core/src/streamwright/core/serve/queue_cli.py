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
`streamwright queue purge|migrate SERVE_YAML`: the operator's upkeep of a service's run queue (the state store
serve.yaml's `state` names). Only `state` is read: no API key, secret of the jobs or manifest is needed, so a cron job
can run it anywhere the database is reachable.
"""

import datetime
import sys

from streamwright.core.cli.commands.state import migrate
from streamwright.core.cli.help_format import DescriptionFormatter
from streamwright.core.cli.job_state import purge_age
from streamwright.core.jobs.state import StateError
from streamwright.core.serve.config import ServiceError, load_state
from streamwright.core.serve.queue_store import open_queue

__all__ = ["register_queue"]

SUCCESS, FAILURE, INVALID_INPUT = 0, 1, 2
_DEFAULT_AGE = datetime.timedelta(days=30)
_QUEUE = """\
Look after the run queue of a service (`streamwright serve`): the state store its serve.yaml's `state` names.

  streamwright queue purge serve.yaml --older-than 30d     # e.g. daily, from cron
  streamwright queue migrate serve.yaml                    # after an upgrade that changes the tables"""


def register_queue(subparsers):
    parser = subparsers.add_parser("queue", help="purge and migrate the run queue of `streamwright serve`",
                                   description=_QUEUE, formatter_class=DescriptionFormatter)
    actions = parser.add_subparsers(dest="queue_action", metavar="ACTION", required=True)
    purge = actions.add_parser("purge", help="delete the runs that ended longer ago than --older-than",
                               description="Delete the ended runs (succeeded, failed) that finished longer ago than "
                                           "--older-than, 10,000 per transaction. Queued and running runs are kept.")
    purge.add_argument("--older-than", type=purge_age, metavar="AGE", default=_DEFAULT_AGE,
                       help="days (30) or a duration (30d, 12h); default 30 days")
    purge.add_argument("--dry-run", action="store_true", help="count the runs it would delete; delete none")
    migrate_parser = actions.add_parser("migrate", help="create or upgrade the queue's tables",
                                        description="Apply the pending migrations of the state store that holds "
                                                    "the run queue (idempotent).")
    migrate_parser.add_argument("--dry-run", action="store_true", help="print the SQL; change nothing")
    for action in (purge, migrate_parser):
        action.add_argument("serve_yaml", metavar="SERVE_YAML", help="the service whose run queue it is")
        action.set_defaults(command_handler=cmd_queue)


def _purge(store, args):
    cutoff = datetime.datetime.now(datetime.UTC) - args.older_than
    count = store.purge_requests(cutoff, dry_run=args.dry_run)
    sys.stdout.write("%s %d run(s) that ended before %s in %s\n" % (
        "would purge" if args.dry_run else "purged", count, cutoff.strftime("%Y-%m-%dT%H:%M:%SZ"), store.target))
    return SUCCESS


def cmd_queue(args):
    try:
        store = open_queue(load_state(args.serve_yaml))
        if args.queue_action == "migrate":
            return migrate(store.migrations(), args.dry_run)
        store.prepare(migrate=False)
        return _purge(store, args)
    except ServiceError as exc:
        sys.stderr.writelines("streamwright queue: %s\n" % problem for problem in exc.problems)
        return INVALID_INPUT
    except (ValueError, StateError) as exc:
        sys.stderr.write("streamwright queue: %s\n" % exc)
        return FAILURE
