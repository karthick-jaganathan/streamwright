"""Export schemas, records and partial-load markers to the output contract."""

from streamwright.core.engine import sql


class ExportWriter:
    def __init__(self, output, partition_value):
        self.output = output
        self.partition_value = partition_value

    def write_schema(self, export, columns, keys, stats=None):
        self.output.write_columns(export, columns)
        self.output.write_schema(export, sql.table_schema(columns), list(keys))
        if stats is not None:
            stats.exports.setdefault(export, 0)

    def write_records(self, export, records, wanted, stats):
        written = 0
        try:
            for record in records:
                self.output.write_record(export, record)
                written += 1
                for fields, rows in wanted.items():
                    row = tuple(self.partition_value(record.get(field)) for field in fields)
                    if all(value is not None for value in row):
                        rows.add(row)
        finally:
            stats.exports[export] = stats.exports.get(export, 0) + written

    def mark_partial(self, plan):
        for export, _, _ in plan.exports:
            self.output.mark_partial(export)
