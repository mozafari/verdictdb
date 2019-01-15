# Creating/Viewing Scrambles

A scramble is a special table used by VerdictDB to speed up query processing. The information about the created scrambles is stored in its metadata table and is used at query time.


## Syntax for Creating Scrambles

```sql
CREATE SCRAMBLE (IF NOT EXISTS)? scrambled_table=table_name FROM original_table=table_name
(WHERE where=search_condition)?
(METHOD method=scrambling_method_name)?
((HASHCOLUMN | ON) hash_column=column_name)?
((SIZE | RATIO) percent=FLOAT)?
(BLOCKSIZE blocksize=DECIMAL)?
```

Note:

1. `newSchema` may be identical to `originalSchema`.
1. `newTable` must be different from `originalTable` if `newSchema` is same as `originalSchema`.
1. The user requires the write privilege for the `newSchema` schema and the read privilege for the `originalSchema.originalTable`.
1. METHOD must be either "uniform" or "hash". A uniform scramble is used for count, sum, avg, max, and min. A hash scramble is used for count-distinct.
1. If a hash scramble is to be built, the `hashcolumn` must be present. `hashcolumn` indicates the column that will appear within the count-distinct function (e.g., `count(distinct hashcolumn)`).
1. `sizeOfScramble` (default = 1.0) defines the relative size of the scramble to its original table and must be a float value between 0.0 and 1.0 (e.g., `sizeOfScramble=0.1` will create a scramble which size is 10% of the original table).
<!-- 1. VerdictDB stores scrambles in a partitioned table and `sizeOfBlock` (default = 1,000,000 = 1M) specifies the number of records in each partition. Note that VerdictDB sets the maximum number of partitions for scrambles is 100 by default, and `sizeOfBlock` will be adjusted automatically by VerdictDB if the specified `sizeOfBlock` results in more than 100 partitions. -->
1. The schema and table names can be quoted either using the double-quote (") or the backtick (\`).


## Syntax for Viewing Scrambles

```sql
SHOW SCRAMBLES;
```

This query will print the list of all scrambled tables that have been built.
