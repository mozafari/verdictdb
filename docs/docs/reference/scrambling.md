# Creating/Viewing Scrambles

A scramble is a special table used by VerdictDB to speed up query processing. The information about the created scrambles is stored in its metadata table and is used at query time.


## Syntax for Creating Scrambles

```sql
CREATE SCRAMBLE [IF NOT EXISTS] new_schema.scrambled_table
FROM original_schema.original_table
[WHERE condition]
[METHOD {UNIFORM|HASH}]
[{HASHCOLUMN|ON} hash_column]
[{SIZE|RATIO} percent=FLOAT]
[BLOCKSIZE size=DECIMAL]
```

Note:

1. `scrambled_table` must be different from `original_table` if `new_schema` is the same as `original_schema`.
1. The user requires write privilege for `newSchema` and read privilege for `originalSchema.originalTable`.
1. `METHOD` must be either `UNIFORM` or `HASH`. A `UNIFORM` scramble is used for `count`, `sum`, `avg`, `max` and `min` queries. A `HASH` scramble is used for `count distinct` queries. `METHOD` is `UNIFORM` by default.
1. If a `HASH` scramble is to be built, `HASHCOLUMN` or `ON` must be present. `hash_column` indicates the column that will appear within the count-distinct function (e.g., `count(distinct hashcolumn)`).
1. `SIZE` or `RATIO` (`percent = 1.0` by default) defines the relative size of the scramble to its original table and must be a float value between 0.0 and 1.0 (e.g., `percent = 0.1` will create a scramble with 10% size of the original table).
1. VerdictDB stores scrambles in a partitioned table and `BLOCKSIZE` (`size = 1,000,000 = 1M` by default) specifies the number of records in each partition. The maximum number of partitions for scrambles is 100 by default in VerdictDB. If the specified `size` results in more than 100 partitions, it will be adjusted automatically.
1. The schema and table names can be quoted either using the double-quote (") or the backtick (\`).


## Syntax for Viewing Scrambles

```sql
SHOW SCRAMBLES;
```

This query will print the list of all scrambled tables that have been built.
