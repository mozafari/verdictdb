# VerdictDB Documentation

## Introduction

VerdictDB is a thin, platform-independent, interactive analytics library that works on top of your existing (or backend) database system (e.g., MySQL, PostgreSQL, Redshift, etc.). For platform-independence, VerdictDB makes all communications with the backend database in SQL. For interactive querying, VerdictDB intelligently **infers** the query answers based on the results processed on a part of the original data. Those inferred answers are highly accurate estimators of the exact answers. Furthermore, *even when only exact answers are needed*, VerdictDB can also be useful with its **streaming sql engine**. The streaming sql engine provides interactive-speed feedbacks in the process of computing exact answers. See [this page](/how_it_works/basics) if you want to know more about VerdictDB's internal mechanism.


## Workflow Overview

First, users must create a *scramble* for their large table. The scramble is just some table in a special format. Once the scramble is created, VerdictDB performs its unique operations to quickly process aggregate queries involving the large table.


## Contents

1. Getting Started
    - [Quickstart](/getting_started/quickstart)
    - [Install / Download](/getting_started/install)
    - [What's More](/getting_started/whatsmore)
1. How VerdictDB works
    - [Basics](/how_it_works/basics)
    - [Architecture](/how_it_works/architecture)
    - [Query Processing](/how_it_works/query_processing)
1. Tutorial
    - Setting up databases
        - [MySQL](/tutorial/setup/mysql)
        - [Apache Spark](/tutorial/setup/spark)
    - [Setting up TPC-H data](/tutorial/tpch)
    - Example Applications
        - [MySQL](/tutorial/example/mysql)
        - [Apache Spark](/tutorial/example/spark)
1. Reference
    - [Connecting to Databases](/reference/connection)
    - [VerdictDB JDBC Properties](/reference/properties)
    - [Creating/Viewing Scrambles](/reference/scrambling)
    - [Appending Scrambles](/reference/append_scrambling)
    - [Dropping Scrambles](/reference/drop_scrambling)
    - [Select-Query Syntax](/reference/query_syntax)
    - [Stream Querying](/reference/streaming)
    - [Javadoc](/reference/javadoc)
    - [PyVerdict Doc](/reference/pyverdict)



## License and Developments

VerdictDB is under [the Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0); thus, it is completely free for both commercial and non-commercial purposes. VerdictDB is developed by the database group at the University of Michigan, Ann Arbor.
