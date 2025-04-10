"""Basic DB optimizer for Frappe Framework based app.

This is largely based on heuristics and known good practices for indexing.
"""

from __future__ import annotations

import re
from collections import defaultdict
from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal

from sql_metadata import Parser

if TYPE_CHECKING:
    from agent.site import Site

# Any index that reads more than 30% table on average is not "useful"
INDEX_SCORE_THRESHOLD = 0.3
# Anything reading less than this percent of table is considered optimal
OPTIMIZATION_THRESHOLD = 0.1


def cstr(data: str | None) -> str:
    if data is None:
        return ""
    return str(data)


def cint(data: int | str | float | None) -> int:
    if data is None:
        return 0
    if isinstance(data, str):
        if len(data) == 0:
            return 0
        data = data.split(".")[0]  # remove decimals
        data = data.replace(",", "")
    return int(data)


def flt(data: float | None) -> float:
    if data is None:
        return 0.0
    return float(data)


@dataclass
class DBExplain:
    # refer: https://mariadb.com/kb/en/explain/
    # Anything not explicitly encoded here is likely not supported.
    select_type: Literal["SIMPLE", "PRIMARY", "SUBQUERY", "UNION", "DERIVED"]
    table: str
    scan_type: Literal[  # What type of scan will be performed
        "ALL",  # Full table scan
        "CONST",  # Single row will be read
        "EQ_REF",  # A single row is found from *unique* index
        "REF",  # Index is used, but MIGHT hit more than 1 rows as it's non-unique
        "RANGE",  # The table will be accessed with a key over one or more value ranges.
        "INDEX_MERGE",  # multiple indexes are used and merged smartly. Equivalent to RANGE
        "INDEX_SUBQUERY",
        "INDEX",  # Full index scan is performed. Similar to full table scan in case of large number of rows.
        "REF_OR_NULL",
        "UNIQUE_SUBQUERY",
        "FULLTEXT",  # Full text index is used,
    ]
    possible_keys: list[str] | None = None  # possible indexes that can be used
    key: str | None = None  # This index is being used
    key_len: int | None = None  # How many prefix bytes from index are being used
    ref: str | None = None  # is reference constant or some other column
    rows: int = 0  # roughly how many rows will be examined
    extra: str | None = None

    @classmethod
    def from_frappe_output(cls, data) -> DBExplain:
        return cls(
            select_type=cstr(data["select_type"]).upper(),
            table=data["table"],
            scan_type=cstr(data["type"]).upper(),
            possible_keys=data["possible_keys"],
            key=data["key"],
            key_len=cint(data["key_len"]) if data["key_len"] else None,
            ref=data["ref"],
            rows=cint(data["rows"]),
            extra=data.get("Extra"),
        )


@dataclass
class DBColumn:
    name: str
    cardinality: int | None
    is_nullable: bool
    default: str
    data_type: str

    @classmethod
    def from_frappe_output(cls, data) -> DBColumn:
        "Parse DBColumn from output of describe-database-table command in Frappe"
        return cls(
            name=data["column"],
            cardinality=data.get("cardinality"),
            is_nullable=data["is_nullable"],
            default=data["default"],
            data_type=data["type"],
        )


@dataclass
class DBIndex:
    name: str
    column: str
    table: str
    unique: bool | None = None
    cardinality: int | None = None
    sequence: int = 1
    nullable: bool = True
    _score: float = 0.0

    def __eq__(self, other: DBIndex) -> bool:
        return self.column == other.column and self.sequence == other.sequence and self.table == other.table

    def __repr__(self):
        return f"DBIndex(`{self.table}`.`{self.column}`)"

    @classmethod
    def from_frappe_output(cls, data, table) -> DBIndex:
        "Parse DBIndex from output of describe-database-table command in Frappe"
        return cls(
            name=data["name"],
            table=table,
            unique=data["unique"],
            cardinality=data["cardinality"],
            sequence=data["sequence"],
            nullable=data["nullable"],
            column=data["column"],
        )

    def to_dict(self, fields: list[str] | None = None) -> dict:
        if not fields:
            fields = ["name", "column", "table", "unique", "cardinality", "sequence", "nullable"]
        return {field: getattr(self, field) for field in fields}


@dataclass
class ColumnStat:
    column_name: str
    avg_frequency: float
    avg_length: float
    nulls_ratio: float | None = None
    histogram: list[float] = None

    def __post_init__(self):
        if not self.histogram:
            self.histogram = []

    @classmethod
    def from_frappe_output(cls, data) -> ColumnStat:
        return cls(
            column_name=data["column_name"],
            avg_frequency=data["avg_frequency"],
            avg_length=data["avg_length"],
            nulls_ratio=data["nulls_ratio"],
            histogram=[flt(bin) for bin in data["histogram"].split(",")] if data["histogram"] else [],
        )


@dataclass
class DBTable:
    name: str
    total_rows: int
    schema: list[DBColumn] | None = None
    indexes: list[DBIndex] | None = None

    def __post_init__(self):
        if not self.schema:
            self.schema = []
        if not self.indexes:
            self.indexes = []

    def update_cardinality(self, column_stats: list[ColumnStat]) -> None:
        """Estimate cardinality using mysql.column_stat"""
        for column_stat in column_stats:
            for col in self.schema:
                if col.name == column_stat.column_name and not col.cardinality and column_stat.avg_frequency:
                    # "hack" or "math" - average frequency is on average how frequently a row value appears.
                    # Avg = total_rows / cardinality, so...
                    col.cardinality = self.total_rows / column_stat.avg_frequency

    @classmethod
    def from_frappe_output(cls, data) -> DBTable:
        "Parse DBTable from output of describe-database-table command in Frappe"
        table_name = data["table_name"]
        return cls(
            name=table_name,
            total_rows=data["total_rows"],
            schema=[DBColumn.from_frappe_output(c) for c in data["schema"]],
            indexes=[DBIndex.from_frappe_output(i, table_name) for i in data["indexes"]],
        )

    def has_column(self, column: str) -> bool:
        return any(col.name == column for col in self.schema)


@dataclass
class DBOptimizer:
    query: str  # raw query in string format
    explain_plan: list[DBExplain] = None
    tables: dict[str, DBTable] = None
    parsed_query: Parser = None

    def __post_init__(self):
        if not self.explain_plan:
            self.explain_plan = []
        if not self.tables:
            self.tables = {}
        for explain_entry in self.explain_plan:
            explain_entry.select_type = explain_entry.select_type.upper()
            explain_entry.scan_type = explain_entry.scan_type.upper()
        self.parsed_query = Parser(re.sub(r'"(\S+)"', r"'\1'", self.query))

    @property
    def tables_examined(self) -> list[str]:
        return self.parsed_query.tables

    def update_table_data(self, table: DBTable):
        self.tables[table.name] = table

    def potential_indexes(self) -> list[DBIndex]:
        """Get all columns that can potentially be indexed to speed up this query."""

        possible_indexes = []

        # Where claus columns using these operators benefit from index
        #  1. = (equality)
        #  2. >, <, >=, <=
        #  3. LIKE 'xyz%' (Prefix search)
        #  4. BETWEEN (for date[time] fields)
        #  5. IN (similar to equality)
        if where_columns := self.parsed_query.columns_dict.get("where"):
            # TODO: Apply some heuristics here, not all columns in where clause are actually useful
            possible_indexes.extend(where_columns)

        # Join clauses - Both sides of join should ideally be indexed. One will *usually* be primary key.
        if join_columns := self.parsed_query.columns_dict.get("join"):
            possible_indexes.extend(join_columns)

        # Top N query variant - Order by column can possibly speed up the query
        if (
            order_by_columns := self.parsed_query.columns_dict.get("order_by")
        ) and self.parsed_query.limit_and_offset:
            possible_indexes.extend(order_by_columns)

        possible_db_indexes = [self._convert_to_db_index(i) for i in possible_indexes]
        possible_db_indexes = [i for i in possible_db_indexes if i.column not in ("*", "name")]
        possible_db_indexes.sort(key=lambda i: (i.table, i.column))

        return self._remove_existing_indexes(possible_db_indexes)

    def _convert_to_db_index(self, column: str) -> DBIndex:
        column_name, table = None, None

        if "." in column:
            table, column_name = column.split(".")
        else:
            column_name = column
            for table_name, db_table in self.tables.items():
                if db_table.has_column(column):
                    table = table_name
                    break
        return DBIndex(column=column_name, name=column_name, table=table)

    def _remove_existing_indexes(self, potential_indexes: list[DBIndex]) -> list[DBIndex]:  # noqa: C901
        """Given list of potential index candidates remove the ones that already exist.

        This also removes multi-column indexes for parts that are applicable to query.
        Example: If multi-col index A+B+C exists and query utilizes A+B then
        A+B are removed from potential indexes.
        """

        def remove_maximum_indexes(idx: list[DBIndex]):
            """Try to remove entire index from potential indexes
            If not possible, reduce one part and try again until no parts are left.
            """
            if not idx:
                return None
            matched_sub_index = []
            for idx_part in list(idx):
                matching_part = [
                    i for i in potential_indexes if i.column == idx_part.column and i.table == idx_part.table
                ]
                if not matching_part:
                    # pop and recurse
                    idx.pop()
                    return remove_maximum_indexes(idx)
                matched_sub_index.extend(matching_part)

            # Every part matched now, lets remove those parts
            for i in matched_sub_index:
                potential_indexes.remove(i)
            return None

        # Reconstruct multi-col index
        for table in self.tables.values():
            merged_indexes = defaultdict(list)
            for index in table.indexes:
                merged_indexes[index.name].append(index)

            for idx in merged_indexes.values():
                idx.sort(key=lambda x: x.sequence)

            for idx in merged_indexes.values():
                remove_maximum_indexes(idx)
        return potential_indexes

    def suggest_index(self) -> DBIndex | None:
        """Suggest best possible column to index given query and table stats."""
        if missing_tables := (set(self.tables_examined) - set(self.tables.keys())):
            raise Exception("DBTable information missing for: " + ", ".join(missing_tables))

        potential_indexes = self.potential_indexes()

        for index in list(potential_indexes):
            table = self.tables[index.table]

            # Data type is not easily indexable - skip
            column = next(c for c in table.schema if c.name == index.column)
            if "text" in column.data_type.lower() or "json" in column.data_type.lower():
                potential_indexes.remove(index)
            # Update cardinality from column so scoring can be done
            index.cardinality = column.cardinality

        for index in potential_indexes:
            index._score = self.index_score(index)

        potential_indexes.sort(key=lambda i: i._score)
        if (
            potential_indexes
            and (best_index := potential_indexes[0])
            and best_index._score < INDEX_SCORE_THRESHOLD
        ):
            return best_index
        return None

    def index_score(self, index: DBIndex) -> float:
        """Score an index from 0 to 1 based on usefulness.

        A score of 0.5 indicates on average this index will read 50% of the table. (e.g. checkboxes)"""
        table = self.tables[index.table]

        cardinality = index.cardinality or 2
        total_rows = table.total_rows or cardinality or 1

        # We assume most unique values are evenly distributed, this is
        # definitely not the case IRL but it should be good enough assumptions
        # Score is roughly what percentage of table we will end up reading on typical query
        rows_fetched_on_average = (table.total_rows or cardinality) / cardinality
        return rows_fetched_on_average / total_rows

    def can_be_optimized(self) -> bool:
        """Return true if it's worth optimizing.

        Few cases can not be optimized any further. E.g. ref/eq_ref/cost type
        of queries. Assume that anything that reads <10% of table already is
        not possible to truly optimize with these heuristics."""
        for explain in self.explain_plan:
            for table in self.tables.values():
                if table.name != explain.table:
                    continue
                if (explain.rows / table.total_rows) > OPTIMIZATION_THRESHOLD:
                    return True
        return False


class OptimizeDatabaseQueries:
    def __init__(self, site: Site, queries: list[str], database_root_password: str):
        self.site = site
        self.database_root_password = database_root_password
        self.queries = queries
        self.table_cache: dict[str, DBTable] = {}
        self.column_statistics_cache: dict[str, list[ColumnStat]] = {}

    def analyze(self) -> dict[str, list[DBIndex]] | None:
        # generate explain output for all the queries at once
        explain_output_of_queries_result = self.site.db_instance().explain_queries(self.queries)
        explain_output_of_queries = {}
        for query, explain_output in explain_output_of_queries_result.items():
            explain_output_of_queries[query] = [DBExplain.from_frappe_output(e) for e in explain_output]

        suggested_indexes_of_queries = {}

        for query in self.queries:
            if query not in explain_output_of_queries:
                continue
            explain_output = explain_output_of_queries[query]
            optimizer = DBOptimizer(query=query, explain_plan=explain_output)
            tables = optimizer.tables_examined

            for table in tables:
                db_table = self.describe_database_table(table)
                column_stats = self.fetch_column_stats(table)
                db_table.update_cardinality(column_stats)
                optimizer.update_table_data(db_table)

            index = optimizer.suggest_index()
            if index:
                if query not in suggested_indexes_of_queries:
                    suggested_indexes_of_queries[query] = []
                suggested_indexes_of_queries[query].append(index)

        return suggested_indexes_of_queries

    def describe_database_table(self, table_name: str) -> DBTable | None:
        if table_name in self.table_cache:
            return self.table_cache[table_name]
        result = self.site.describe_database_table(table_name)
        if result is None:
            self.table_cache[table_name] = None
            return None
        table = DBTable.from_frappe_output(result)
        self.table_cache[table_name] = table
        return table

    def fetch_column_stats(self, table_name: str) -> list[ColumnStat] | None:
        if table_name in self.column_statistics_cache:
            return self.column_statistics_cache[table_name]
        db = self.site.db_instance("root", self.database_root_password)
        return db.fetch_database_column_statistics(table_name)
