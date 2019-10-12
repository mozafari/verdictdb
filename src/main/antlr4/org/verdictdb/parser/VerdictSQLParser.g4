/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
T-SQL (Transact-SQL, MSSQL) grammar.
The MIT License (MIT).
Copyright (c) 2015-2016, Ivan Kochurkin (kvanttt@gmail.com), Positive Technologies.
Copyright (c) 2016, Scott Ure (scott@redstormsoftware.com).
Copyright (c) 2016, Rui Zhang (ruizhang.ccs@gmail.com).
Copyright (c) 2016, Marcus Henriksson (kuseman80@gmail.com).

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
*/

parser grammar VerdictSQLParser;

options { tokenVocab=VerdictSQLLexer; }

// VERDICT

verdict_statement
    : select_statement
    | stream_select_statement
    | create_scramble_statement
    | insert_scramble_statement
    | drop_scramble_statement
    | drop_all_scrambles_statement
    | show_scrambles_statement
    | config_statement
    | other_statement
    | create_table
    | create_table_as_select
    | create_view
    | drop_table
    | drop_view
    ;

//WITH SIZE size=(FLOAT | DECIMAL) '%' (STORE poission_cols=DECIMAL POISSON COLUMNS)? (STRATIFIED BY column_name (',' column_name)*)?
create_scramble_statement
    : CREATE SCRAMBLE (IF NOT EXISTS)? scrambled_table=table_name FROM original_table=table_name
      (WHERE where=search_condition)?
      (METHOD method=scrambling_method_name)?
      ((HASHCOLUMN | ON) hash_column=column_name)? 
      ((SIZE | RATIO) percent=FLOAT)?
      (BLOCKSIZE blocksize=DECIMAL)?
    ;

insert_scramble_statement
	: (APPEND|INSERT) SCRAMBLE scrambled_table=table_name WHERE where=search_condition
	;

scrambling_method_name
    : config_value
    | HASH
    | UNIFORM
    ;
    
//on_columns
//    : ON column_name (',' column_name)*
//    ;

drop_scramble_statement
    : DROP SCRAMBLE scrambled_table=table_name (ON original_table=table_name)?
    ;

drop_all_scrambles_statement
	: DROP ALL SCRAMBLE original_table=table_name
	;

show_scrambles_statement
    : SHOW SCRAMBLES
    ;

config_statement
    : config_set_statement
    | config_get_statement
    ;

other_statement
    : use_statement
    | show_tables_statement
    | show_databases_statement
    | describe_table_statement
    | refresh_statement
    | show_config_statement
    | create_database
    | drop_database
    ;
    
create_database
    : CREATE ( DATABASE | SCHEMA ) (database=id)
    ;
    
drop_database
    : DROP ( DATABASE | SCHEMA ) ( IF EXISTS )? (database=id)
    ;

//config_set_statement: SET key=config_key '=' value=config_value percent='%'?;
config_set_statement: SET key=config_key '=' value=config_value;

config_get_statement: GET key=config_key;

config_key: ID;

config_value
    : DOUBLE_QUOTE_ID
    | STRING
    | ID (',' ID)*
    ;



//DOUBLE_QUOTE_STRING: '"' (~'"' | '\\"')* '"';


// VERDICT

tsql_file
    : sql_clause* EOF
    ;

sql_clause
    : ddl_clause
    | other_statement
    ;

// Data Definition Language: https://msdn.microsoft.com/en-us/library/ff848799.aspx)
ddl_clause
    : create_table
    | create_view
    | alter_table
    | alter_database
    | drop_table
    | drop_view
    ;

// DML

// https://msdn.microsoft.com/en-us/library/ms189499.aspx
select_statement
    : with_expression? EXACT? query_expression order_by_clause? limit_clause? use_original_after_clause? ';'?
    ;

stream_select_statement
   : STREAM select_statement
   ;

// https://msdn.microsoft.com/en-us/library/ms177564.aspx
output_clause
    : OUTPUT output_dml_list_elem (',' output_dml_list_elem)*
            (INTO (LOCAL_ID | table_name) ('(' column_name_list ')')? )?
    ;

output_dml_list_elem
    : (output_column_name | expression) (AS? column_alias)?  // TODO: scalar_expression
    ;

output_column_name
    : (DELETED | INSERTED | table_name) '.' ('*' | column_name)
    | DOLLAR_ACTION
    ;

// DDL

// https://msdn.microsoft.com/en-us/library/ms174979.aspx
create_table
    : CREATE TABLE table_name '(' column_def_table_constraint (','? column_def_table_constraint)* ','? ')'
    ;

create_table_as_select
    : CREATE TABLE (IF NOT EXISTS)? table_name STORED_AS_PARQUET? AS select_statement ';'?
    ;

// https://msdn.microsoft.com/en-us/library/ms187956.aspx
create_view
    : CREATE VIEW view_name ('(' column_name (',' column_name)* ')')?
      AS select_statement (WITH CHECK OPTION)? ';'?
    ;

// https://msdn.microsoft.com/en-us/library/ms190273.aspx
alter_table
    : ALTER TABLE table_name SET '(' LOCK_ESCALATION '=' (AUTO | TABLE | DISABLE) ')' ';'?
    | ALTER TABLE table_name ADD column_def_table_constraint ';'?
    ;

// https://msdn.microsoft.com/en-us/library/ms174269.aspx
alter_database
    : ALTER DATABASE (database=id | CURRENT)
      MODIFY NAME '=' new_name=id ';'?
    ;

// https://msdn.microsoft.com/en-us/library/ms173790.aspx
drop_table
    : DROP TABLE (IF EXISTS)? table_name ';'?
    ;

// https://msdn.microsoft.com/en-us/library/ms173492.aspx
drop_view
    : DROP VIEW (IF EXISTS)? view_name (',' view_name)* ';'?
    ;

// Other statements.

// https://msdn.microsoft.com/en-us/library/ms189484.aspx
set_statment
    : SET LOCAL_ID ('.' member_name=id)? '=' expression ';'?
    | SET LOCAL_ID assignment_operator expression ';'?
    // https://msdn.microsoft.com/en-us/library/ms189837.aspx
    | set_special
    ;

// https://msdn.microsoft.com/en-us/library/ms188366.aspx
use_statement
    : USE database=id ';'?
    ;

show_tables_statement
    : SHOW TABLES (IN schema=id)? ';'?
    ;

show_databases_statement
    : SHOW ( DATABASES | SCHEMAS )
    ;

describe_table_statement
    : DESCRIBE table=table_name ';'?
    ;
    
refresh_statement
    : REFRESH (schema=id)? ';'?
    ;
    
show_config_statement
    : SHOW CONFIG ';'?
    ;

table_type_definition
    : TABLE '(' column_def_table_constraint (','? column_def_table_constraint)* ')'
    ;

column_def_table_constraint
    : column_definition
    | table_constraint
    ;

// https://msdn.microsoft.com/en-us/library/ms187742.aspx
column_definition
    : column_name (data_type | AS expression) null_notnull?
    ;

// https://msdn.microsoft.com/en-us/library/ms186712.aspx
column_constraint
    :(CONSTRAINT id)? null_notnull?
    ;

// https://msdn.microsoft.com/en-us/library/ms188066.aspx
table_constraint
    : (CONSTRAINT id)? '(' column_name_list ')'
    ;

// https://msdn.microsoft.com/en-us/library/ms190356.aspx
// Runtime check.
set_special
    : SET id (id | constant | LOCAL_ID | on_off) ';'?
    // https://msdn.microsoft.com/en-us/library/ms173763.aspx
    | SET TRANSACTION ISOLATION LEVEL
      (READ UNCOMMITTED | READ COMMITTED | REPEATABLE READ | SNAPSHOT | SERIALIZABLE) ';'?
    // https://msdn.microsoft.com/en-us/library/ms188059.aspx
    | SET IDENTITY_INSERT table_name on_off ';'?
    ;

// Expression.

// https://msdn.microsoft.com/en-us/library/ms190286.aspx
// Operator precendence: https://msdn.microsoft.com/en-us/library/ms190276.aspx
expression
    : NULL                                                     #primitive_expression
    | LOCAL_ID                                                 #primitive_expression
    | constant                                                 #primitive_expression
    | true_orfalse                                             #primitive_expression
    | case_expr                                                #case_expression
    | full_column_name                                         #column_ref_expression
    | '(' expression ')'                                       #bracket_expression
    | '(' subquery ')'                                         #subquery_expression
    | '~' expression                                           #unary_operator_expression
    | expression op=('*' | '/' | '%') expression               #binary_operator_expression
    | op=('+' | '-') expression                                #unary_operator_expression
    | expression op=('+' | '-' | '&' | '^' | '|' | '#' | '||' | '<<' | '>>'  ) expression   #binary_operator_expression
    | expression comparison_operator expression                #binary_operator_expression
    | NOT expression                                           #not_expression
    | expression IS null_notnull                               #is_null_expression
    | interval                                                 #interval_expression
    | date                                                     #date_expression
    | function_call                                            #function_call_expression
    | expression COLLATE id                                    #function_call_expression
    ;

interval
	: INTERVAL constant_expression (DAY | DAYS | MONTH | MONTHS | YEAR | YEARS)
	;

date
    : DATE constant_expression
    ;

constant_expression
    : NULL
    | constant
    // system functions: https://msdn.microsoft.com/en-us/library/ms187786.aspx
//    | function_call
    | LOCAL_ID         // TODO: remove.
    | '(' constant_expression ')'
    ;

subquery
    : select_statement
    ;

dml_table_source
    : query_specification
    ;

// https://msdn.microsoft.com/en-us/library/ms175972.aspx
with_expression
    : WITH (XMLNAMESPACES ',')? common_table_expression (',' common_table_expression)*
    ;

common_table_expression
    : expression_name=id ('(' column_name_list ')')? AS '(' select_statement ')'
    ;

update_elem
    : (full_column_name | LOCAL_ID) ('=' | assignment_operator) expression
    | udt_column_name=id '.' method_name=id '(' expression_list ')'
    //| full_column_name '.' WRITE (expression, )
    ;

// https://msdn.microsoft.com/en-us/library/ms173545.aspx
search_condition_list
    : search_condition (',' search_condition)*
    ;

search_condition
    : search_condition_or (AND search_condition_or)*
    ;

search_condition_or
    : search_condition_not (OR search_condition_not)*
    ;

search_condition_not
    : NOT? predicate
    ;

predicate
    : EXISTS '(' subquery ')'                                               # exists_predicate
    | '(' search_condition ')'                                              # bracket_predicate
    | predicate comparison_operator expression                              # comp_pred_expr_predicate
    | expression comparison_operator expression                             # comp_expr_predicate
    | expression comparison_operator (ALL | SOME | ANY) '(' subquery ')'    # setcomp_expr_predicate
    | expression NOT? BETWEEN expression AND expression                     # comp_between_expr
    | expression NOT? IN '(' (subquery | expression_list) ')'               # in_predicate
    | expression NOT? (LIKE | RLIKE) expression (ESCAPE expression)?                  # like_predicate
    | expression IS null_notnull                                            # is_predicate
    | unary_predicate_function                                               # unary_func_predicate
    | binary_predicate_function                                               # binary_func_predicate
    ;

query_expression
    : (query_specification | '(' query_expression ')') union*
    ;

union
    : (UNION ALL? | EXCEPT | INTERSECT) (query_specification | ('(' query_expression ')')+)
    ;

// https://msdn.microsoft.com/en-us/library/ms176104.aspx
query_specification
    : SELECT (ALL | DISTINCT)? top_clause? // (TOP expression PERCENT? (WITH TIES)?)?
      select_list
      // https://msdn.microsoft.com/en-us/library/ms188029.aspx
      (INTO into_table=table_name)?
      (FROM (table_source (',' table_source)*))?
      (WHERE where=search_condition)?
      // https://msdn.microsoft.com/en-us/library/ms177673.aspx
      ((GROUP BY group_by_item (',' group_by_item)* (WITH ROLLUP)?) |
      (GROUP BY ROLLUP '(' group_by_item (',' group_by_item)* ')'))?
      (HAVING having=search_condition)?
    ;

top_clause
	: TOP number
	;

limit_clause
    : LIMIT number
    ;

use_original_after_clause
    : USE_ORIGINAL_AFTER number
    ;

// https://msdn.microsoft.com/en-us/library/ms188385.aspx
order_by_clause
    : ORDER BY order_by_expression (',' order_by_expression)*
      (OFFSET expression (ROW | ROWS) (FETCH (FIRST | NEXT) expression (ROW | ROWS) ONLY)?)?
    ;

// https://msdn.microsoft.com/en-us/library/ms173812.aspx
for_clause
    : FOR BROWSE
    | FOR XML AUTO xml_common_directives?
    | FOR XML PATH ('(' STRING ')')? xml_common_directives?
    ;

xml_common_directives
    : ',' (BINARY BASE64 | TYPE | ROOT)
    ;

order_by_expression
    : expression (ASC | DESC)? (NULLS FIRST | NULLS LAST)?
    ;

group_by_item
    : expression
    /*| rollup_spec
    | cube_spec
    | grouping_sets_spec
    | grand_total*/
    ;

option_clause
    // https://msdn.microsoft.com/en-us/library/ms181714.aspx
    : OPTION '(' option (',' option)* ')'
    ;

option
    : FAST number_rows=DECIMAL
    | (HASH | ORDER) GROUP
    | (MERGE | HASH | CONCAT) UNION
    | KEEPFIXED PLAN
    | OPTIMIZE FOR '(' optimize_for_arg (',' optimize_for_arg)* ')'
    | OPTIMIZE FOR UNKNOWN
    ;

optimize_for_arg
    : LOCAL_ID (UNKNOWN | '=' constant)
    ;

// https://msdn.microsoft.com/en-us/library/ms176104.aspx
select_list
    : select_list_elem (',' select_list_elem)*
    ;

select_list_elem
    : (table_name '.')? (STAR | '$' (IDENTITY | ROWGUID))
    | column_alias '=' expression
    | expression (AS? column_alias)?
    ;

partition_by_clause
    : PARTITION BY expression_list
    ;

// https://msdn.microsoft.com/en-us/library/ms177634.aspx
table_source
    : table_source_item_joined
    | '(' table_source_item_joined ')'
    ;

table_source_item_joined
    : table_source_item join_part*
    ;

table_source_item
    : table_name_with_hint        as_table_alias?                           # hinted_table_name_item
    | derived_table              (as_table_alias column_alias_list?)?       # derived_table_source_item
    ;

change_table
	: CHANGETABLE '(' CHANGES table_name ',' (NULL | DECIMAL | LOCAL_ID) ')'
	;

// https://msdn.microsoft.com/en-us/library/ms191472.aspx
join_part
    // https://msdn.microsoft.com/en-us/library/ms173815(v=sql.120).aspx
    : (INNER? |
       join_type=(LEFT | RIGHT | FULL) OUTER?) (join_hint=(LOOP | HASH | MERGE | REMOTE | SEMI))?
       JOIN table_source (ON search_condition)?
    | CROSS APPLY table_source
    | CROSS JOIN table_source
    | OUTER APPLY table_source
    | LATERAL VIEW lateral_view_function table_alias? (AS? column_alias)?
    ;

table_name_with_hint
    : table_name
    ;

// https://msdn.microsoft.com/en-us/library/ms190312.aspx
rowset_function
    : OPENROWSET '(' BULK data_file=STRING ',' (bulk_option (',' bulk_option)* | id)')'
    ;

// runtime check.
bulk_option
    : id '=' (DECIMAL | STRING)
    ;

derived_table
    : '(' subquery ')'
    | table_value_constructor
    ;

function_call
    : ranking_windowed_function
    | expression_function
    | aggregate_windowed_function
    ;

datepart
    : ID
    ;

as_table_alias
    : AS? table_alias
    ;

table_alias
    : id
    ;

// Id runtime check. Id can be (FORCESCAN, HOLDLOCK, NOLOCK, NOWAIT, PAGLOCK, READCOMMITTED,
// READCOMMITTEDLOCK, READPAST, READUNCOMMITTED, REPEATABLEREAD, ROWLOCK, TABLOCK, TABLOCKX
// UPDLOCK, XLOCK)

index_column_name
	: ID
	;

index_value
    : ID | DECIMAL
    ;

column_alias_list
    : '(' column_alias (',' column_alias)* ')'
    ;

column_alias
    : id
    | STRING
    ;

table_value_constructor
    : VALUES '(' expression_list ')' (',' '(' expression_list ')')*
    ;

expression_list
    : expression (',' expression)*
    ;

// https://msdn.microsoft.com/en-us/library/ms181765.aspx
case_expr
    : CASE expression (WHEN expression THEN expression)+ (ELSE expression)? END
    | CASE (WHEN search_condition THEN expression)+ (ELSE expression)? END
    ;

// https://msdn.microsoft.com/en-us/library/ms189798.aspx
ranking_windowed_function
    : RANK '(' ')' over_clause
    | DENSE_RANK '(' ')' over_clause
    | NTILE '(' expression ')' over_clause
    | ROW_NUMBER '(' ')' over_clause
    ;

expression_function
    : unary_function
    | noparam_function
    | binary_function
    | ternary_function
    | nary_function
    | unary_predicate_function
    | binary_predicate_function
    | timestamp_function
    | dateadd_function
    | extract_time_function
    | overlay_string_function
    | substring_string_function
    ;

extract_time_function
    : function_name=EXTRACT
      '(' extract_unit FROM expression ')'
    ;

extract_unit
    : YEAR | MONTH | DAY | HOUR | MINUTE | expression
    ;

time_unit
    : YEAR | MONTH | DAY | HOUR | MINUTE | expression
    ;

overlay_string_function
    : function_name=OVERLAY
      '(' expression PLACING expression FROM expression (FOR expression)? ')'
    ;

substring_string_function
    : function_name=SUBSTRING
      '(' expression FROM expression (FOR expression)? ')'
    ;

nary_function
	: function_name=(CONCAT | CONCAT_WS | COALESCE | FIELD | GREATEST | LEAST | WIDTH_BUCKET | BTRIM | FORMAT
	| REGEXP_MATCHES | REGEXP_REPLACE | REGEXP_SPLIT_TO_ARRAY | REGEXP_SPLIT_TO_TABLE | LTRIM | RTRIM | TO_ASCII
	| MAKE_TIMESTAMP | MAKE_TIMESTAMPTZ | TS_HEADLINE | TS_RANK | TS_RANK_CD | UNNEST | XMLCONCAT | XMLELEMENT
	| XMLFOREST | JSON_BUILD_ARRAY | JSON_BUILD_OBJECT | JSONB_SET | JSONB_INSERT | UNNEST
	| GREAT_CIRCLE_DISTANCE | BING_TILES_AROUND
    | REGEXP_EXTRACT_ALL | REGEXP_EXTRACT)
		'(' expression (',' expression)* ')'
    ;

ternary_function
    : function_name=(IF | CONV | SUBSTR | HASH | RPAD | SUBSTRING | LPAD | MID | REPLACE | SUBSTRING_INDEX | MAKETIME | IF
    | CONVERT | SPLIT_PART | TRANSLATE | MAKE_DATE | MAKE_TIME | SETWEIGHT | TS_REWRITE | TSQUERY_PHRASE | XMLROOT
    | XPATH | XPATH_EXISTS | ARRAY_REPLACE | ARRAY_TO_STRING | STRING_TO_ARRAY | LOCATE
    | BING_TILE | BING_TILE_AT | BING_TILES_AROUND)
      '(' expression ',' expression ',' expression ')'
    ;

binary_function
    : function_name=(ROUND | MOD | PMOD | LEFT | RIGHT | STRTOL | POW | POWER | PERCENTILE | SPLIT | INSTR | ENCODE | DECODE | SHIFTLEFT
    | SHIFTRIGHT | SHIFTRIGHTUNSIGNED | NVL | FIND_IN_SET | FORMAT_NUMBER | FORMAT | GET_JSON_OBJECT | IN_FILE
    | LOCATE | REPEAT | AES_ENCRYPT | AES_DECRYPT | POSITION | STRCMP | TRUNCATE | ADDDATE | ADDTIME | DATEDIFF | DATE_ADD
    | DATE_FORMAT | DATE_SUB | MAKEDATE | PERIOD_ADD | PERIOD_DIFF | SUBDATE | TIME_FORMAT | TIMEDIFF | CONVERT | IFNULL | NULLIF
    | DIV | LOG | TRUNC | CONVERT_FROM | CONVERT_TO | LENGTH | STRPOS | GET_BIT | GET_BYTE | SET_BIT | SET_BYTE | TO_CHAR
    | TO_NUMBER | TO_TIMESTAMP | AGE | DATE_PART | DATE_TRUNC | ENUM_RANGE | BOUND_BOX | CIRCLE | POINT | SET_MASKLEN
    | INET_SAME_FAMILY | INET_MERGE | PLAINTO_TSQUERY | PHRASETO_TSQUERY | SETWEIGHT | TO_TSQUERY | TO_TSVECTOR | TS_DELETE
    | TS_FILTER | TS_REWRITE | TSQUERY_PHRASE | XMLPI | XMLROOT | XPATH | XPATH_EXISTS | ARRAY_TO_JSON | ROW_TO_JSON
    | JSON_OBJECT | JSON_EXTRACT_PATH | JSON_EXTRACT_PATH_TEXT | JSON_POPULATE_RECORDSET | JSON_POPULATE_RECORD | SETVAL
    | ARRAY_APPEND | ARRAY_CAT | ARRAY_LENGTH | ARRAY_LOWER | ARRAY_POSITION | ARRAY_POSITIONS | ARRAY_PREPEND
    | ARRAY_REMOVE | ARRAY_TO_STRING | ARRAY_UPPER | STRING_TO_ARRAY | RANGE_MERGE | CORR | COVAR_POP | COVAR_SAMP
    | REGR_AVGX | REGR_AVGY | REGR_COUNT | REGR_INTERCEPT | REGR_R2 | REGR_SLOPE | REGR_SXX | REGR_SXY | REGR_SYY | SUBSTR
    | STDDEV_POP | VARIANCE | VAR_POP | VAR_SAMP | INT4LARGER | SUBSTRING
    | ST_POINT | ST_CONTAINS | ST_CROSSES | ST_DISJOINT
    | ST_EQUALS | ST_INTERSECTS | ST_OVERLAPS | ST_RELATE | ST_TOUCHES | ST_WITHIN | ST_BUFFER | ST_DIFFERENCE
    | ST_INTERSECTION | ST_SYMDIFFERENCE | ST_UNION | ST_DISTANCE | ST_GEOMETRYN | ST_INTERIORRINGN | ST_POINTN
    | SIMPLIFY_GEOMETRY | LINE_LOCATE_POINT | GEOMETRY_TO_BING_TILES
    | REGEXP_LIKE | REGEXP_SPLIT)
      '(' expression ',' expression ')'
    ;

unary_function
    : function_name=(ISNULL | ROUND | CHAR_LENGTH | FLOOR | CEIL | CEILING | EXP | LN | LOG | LOG10 
     | LOG2 | SIN | COS | COT | TAN | SIGN | RAND | FNV_HASH | RAWTOHEX | ABS | STDDEV | SQRT 
     | LCASE | MD5 | CRC32 | YEAR | QUARTER | MONTH | DAY | HOUR | MINUTE | SECOND | WEEKOFYEAR 
     | LOWER | UPPER | UCASE | ASCII | CHARACTER_LENGTH | FACTORIAL | CBRT | LENGTH | TRIM | ASIN 
     | ACOS | ATAN | ATAN2 | DEGREES | RADIANS | POSITIVE | NEGATIVE | BROUND | BIN | HEX | UNHEX 
     | FROM_UNIXTIME | TO_DATE | CHR | LTRIM | RTRIM| REVERSE | SPACE_FUNCTION | SHA1 | SHA2 
     | SPACE | DATE | DAYNAME | DAYOFMONTH | DAYOFWEEK | DAYOFYEAR | FROM_DAYS | LAST_DAY 
     | MICROSECOND | MONTHNAME | SEC_TO_TIME | STR_TO_DATE | TIME | TIME_TO_SEC | TIMESTAMP 
     | TO_DAYS | WEEK | WEEKDAY | YEARWEEK | BINARY | SCALE | TRUNC | SETSEED | BIT_LENGTH 
     | OCTET_LENGTH | CHR | INITCAP | QUOTE_IDENT | QUOTE_LITERAL | QUOTE_NULLABLE | TO_HEX | AGE 
     | ISFINITE | JUSTIFY_DAYS | JUSTIFY_HOURS | JUSTIFY_INTERVALS | TO_TIMESTAMP | ENUM_RANGE 
     | ENUM_FIRST | ENUM_LAST | AREA | CENTER | DIAMETER | HEIGHT | ISCLOSED | ISOPEN | NPOINTS 
     | PCLOSE | POPEN | RADIUS | WIDTH | BOX | CIRCLE | LINE | LSEG | PATH | POINT | POLYGON 
     | ABBREV | BROADCAST | FAMILY | HOST | HOSTMASK | MASKLEN | NETMASK | NETWORK | TEXT 
     | MACADDR8_SET7BIT | ARRAY_TO_TSVECTOR | NUMNODE | PLAINTO_TSQUERY | PHRASETO_TSQUERY 
     | QUERYTREE | STRIP | TO_TSQUERY | TO_TSVECTOR | TSVECTOR_TO_ARRAY | XMLCOMMENT | XMLPI 
     | XMLAGG | XML_ISWELL_FORMAT | TO_JSON | TO_JSONB | ARRAY_TO_JSON | ROW_TO_JSON | JSON_OBJECT 
     | JSON_ARRAY_LENGTH | JSON_EACH | JSON_EACH_TEXT | JSON_OBJECT_KEYS | JSON_ARRAY_ELEMENTS 
     | JSON_ARRAY_ELEMENTS_TEXT | JSON_TYPEOF | JSON_TO_RECORD | JSON_TO_RECORDSET 
     | JSON_STRIP_NULLS | JSONB_PRETTY | CURRVAL | NEXTVAL | ARRAY_NDIMS | ARRAY_DIMS | CARDINALITY 
     | ISEMPTY | LOWER_INC | UPPER_INC | LOWER_INF | UPPER_INF | ARRAY_AGG | BIT_AND | BIT_OR 
     | BOOL_AND | BOOL_OR | EVERY | JSON_AGG | JSONB_AGG | JSON_OBJECT_AGG | JSONB_OBJECT_AGG 
     | STRING_AGG | ST_ASBINARY | ST_ASTEXT | ST_GEOMETRYFROMTEXT | ST_GEOMFROMBINARY 
     | ST_LINEFROMTEXT | ST_LINESTRING | ST_MULTIPOINT | ST_POLYGON | GEOMETRY_UNION | ST_BOUNDARY 
     | ST_ENVELOPE | ST_ENVELOPEASPTS | ST_EXTERIORRING | ST_AREA | ST_CENTROID | ST_CONVEXHULL 
     | ST_DIMENSION | ST_GEOMETRYTYPE | ST_ISCLOSED | ST_ISEMPTY | ST_ISSIMPLE | ST_ISRING 
     | ST_ISVALID | ST_LENGTH | ST_XMAX | ST_YMAX | ST_XMIN | ST_YMIN | ST_STARTPOINT | ST_ENDPOINT 
     | ST_X | ST_Y | ST_INTERIORRINGS | ST_NUMGEOMETRIES | ST_GEOMETRIES | ST_NUMPOINTS 
     | ST_NUMINTERIORRING | GEOMETRY_INVALID_REASON | CONVEX_HULL_AGG | GEOMETRY_UNION_AGG 
     | BING_TILE | BING_TILE_COORDINATES | BING_TILE_POLYGON | BING_TILE_QUADKEY 
     | BING_TILE_ZOOM_LEVEL | APPROX_DISTINCT ) 
       '(' expression ')'
     | function_name=CAST '(' cast_as_expression ')'
    ;

timestamp_function
    : function_name=TIMESTAMP expression
    ;

dateadd_function
	: function_name=DATEADD '(' time_unit ',' expression ',' expression ')'
	;

unary_predicate_function
    : NOT? function_name = (ISNULL | ST_ISCLOSED | ST_ISEMPTY | ST_ISRING | ST_ISVALID)
      '(' expression ')'
    ;

binary_predicate_function
    : NOT? function_name = (ST_CONTAINS | ST_CROSSES | ST_DISJOINT | ST_EQUALS | ST_INTERSECTS
    | ST_OVERLAPS | ST_RELATE | ST_TOUCHES | ST_WITHIN | REGEXP_LIKE)
      '(' expression ',' expression ')'
    ;

noparam_function
    : function_name=(UNIX_TIMESTAMP | CURRENT_TIMESTAMP | CURRENT_DATE | CURRENT_TIME | RANDOM | RAND | NATURAL_CONSTANT
    | PI | CURDATE | CURTIME | LOCALTIME | LOCALTIMESTAMP | NOW | SYSDATE | CURRENT_USER | DATABASE | LAST_INSERT_ID
    | SESSION_USER | SYSTEM_USER | USER | VERSION | PG_CLIENT_ENCODING | CLOCK_TIMESTAMP | STATEMENT_TIMESTAMP
    | TIMEOFDAY | TRANSACTION_TIMESTAMP | GET_CURRENT_TS_CONFIG | TSVECTOR_UPDATE_TRIGGER | TSVECTOR_UPDATE_TRIGGER_COLUMN
    | LASTVAL)
      '(' ')'
    ;
    
lateral_view_function
    : function_name=EXPLODE
      '(' expression ')'
    ;


// https://msdn.microsoft.com/en-us/library/ms173454.aspx
aggregate_windowed_function
    : AVG '(' all_distinct_expression ')' over_clause?
    | CHECKSUM_AGG '(' all_distinct_expression ')'
    | GROUPING '(' expression ')'
    | GROUPING_ID '(' expression_list ')'
    | MAX '(' all_distinct_expression ')' over_clause?
    | MIN '(' all_distinct_expression ')' over_clause?
    | SUM '(' all_distinct_expression ')' over_clause?
    | STDEV '(' all_distinct_expression ')' over_clause?
    | STDEVP '(' all_distinct_expression ')' over_clause?
    | STDDEV_SAMP '(' all_distinct_expression ')' over_clause?
    | VAR '(' all_distinct_expression ')' over_clause?
    | VARP '(' all_distinct_expression ')' over_clause?
    | COUNT '(' ('*' | all_distinct_expression) ')' over_clause?
    | NDV '(' all_distinct_expression ')' over_clause?
    | COUNT_BIG '(' ('*' | all_distinct_expression) ')' over_clause?
    ;

all_distinct_expression
    : (ALL | DISTINCT)? expression
    ;
    
cast_as_expression
    : expression AS data_type
    ;

// https://msdn.microsoft.com/en-us/library/ms189461.aspx
over_clause
    : OVER '(' partition_by_clause? order_by_clause? row_or_range_clause? ')'
    ;

row_or_range_clause
    : (ROWS | RANGE) window_frame_extent
    ;

window_frame_extent
    : window_frame_preceding
    | BETWEEN window_frame_bound AND window_frame_bound
    ;

window_frame_bound
    : window_frame_preceding
    | window_frame_following
    ;

window_frame_preceding
    : UNBOUNDED PRECEDING
    | DECIMAL PRECEDING
    | CURRENT ROW
    ;

window_frame_following
    : UNBOUNDED FOLLOWING
    | DECIMAL FOLLOWING
    ;

// Primitive.

full_table_name
    : (server=id '.' database=id '.'  schema=id   '.'
      |              database=id '.' (schema=id)? '.'
      |                               schema=id   '.')? table=id
    ;

table_name
    : ((catalog=id '.' schema=id '.') | (schema=id '.'))? table=id
    ;

//table_name
//    : (database=id '.' (schema=id)? '.' | schema=id '.')? table=id
//    ;

view_name
    : (schema=id '.')? view=id
    ;

func_proc_name
    : (database=id '.' (schema=id)? '.' | (schema=id) '.')? procedure=id
    ;

ddl_object
    : full_table_name
    | LOCAL_ID
    ;

full_column_name
    : (table_name '.')? column_name
    ;

column_name_list
    : column_name (',' column_name)*
    ;

column_name
    : id
    ;

cursor_name
    : id
    | LOCAL_ID
    ;

on_off
    : ON
    | OFF
    ;

clustered
    : CLUSTERED
    | NONCLUSTERED
    ;

null_notnull
    : NOT? NULL
    ;
    
true_orfalse
    : TRUE | FALSE
    ;

scalar_function_name
    : func_proc_name
    | RIGHT
    | LEFT
    | BINARY_CHECKSUM
    | CHECKSUM
    ;

// https://msdn.microsoft.com/en-us/library/ms187752.aspx
// TODO: implement runtime check or add new tokens.
data_type
    : BIGINT
    | BINARY '(' DECIMAL ')'
    | BIT
    | CHAR
    | CHAR '(' DECIMAL ')'
    | DATE
    | DATETIME
    | DATETIME2
    | DATETIMEOFFSET '(' DECIMAL ')'
    | DECIMAL '(' DECIMAL ',' DECIMAL ')'
    | DOUBLE PRECISION?
    | FLOAT
    | GEOGRAPHY
    | GEOMETRY
    | HIERARCHYID
    | IMAGE
    | INT
    | MONEY
    | NCHAR '(' DECIMAL ')'
    | NTEXT
    | NUMERIC '(' DECIMAL ',' DECIMAL ')'
    | NVARCHAR '(' DECIMAL | MAX ')'
    | REAL
    | SMALLDATETIME
    | SMALLINT
    | SMALLMONEY
    | SQL_VARIANT
    | TEXT
    | TIME '(' DECIMAL ')'
    | TIMESTAMP
    | TIMESTAMP WITHOUT TIME ZONE
    | TINYINT
    | UNIQUEIDENTIFIER
    | VARBINARY '(' DECIMAL | MAX ')'
    | VARCHAR '(' DECIMAL | MAX ')'
    | XML
    | id IDENTITY? ('(' (DECIMAL | MAX) (',' DECIMAL)? ')')?
    ;

default_value
    : NULL
    | constant
    ;

// https://msdn.microsoft.com/en-us/library/ms179899.aspx
constant
    : STRING     // string, datetime or uniqueidentifier
    | BINARY
    | number
    | sign? (REAL | FLOAT)  // float or decimal
    | sign? '$' (DECIMAL | FLOAT)       // money
    ;

number
    : sign? DECIMAL
    ;

sign
    : PLUS
    | MINUS
    ;

// https://msdn.microsoft.com/en-us/library/ms175874.aspx
id
    : simple_id
    | DOUBLE_QUOTE_ID
    | SQUARE_BRACKET_ID
    | BACKTICK_ID
    ;

// This is required to make them to be identified both as identifiers (e.g., column names)
// or function names.
simple_id
    : ID
    | AGE
    | AREA
    | CENTER
    | CIRCLE
    | COUNT
    | DATE
    | DAY
    | DAYNAME
    | DAYOFMONTH
    | DAYOFWEEK
    | DAYOFYEAR
    | DEGREES
    | DIAMETER
    | DISTINCT
    | HEIGHT
    | HOUR
    | LEFT
    | LENGTH
    | MAKEDATE
    | MICROSECOND
    | MINUTE
    | MOD
    | MONTH
    | MONTHNAME
    | RIGHT
    | POWER
    | SCALE
    | SECOND
    | TEXT
    | TIME
    | TIMESTAMP
    | VARIANCE
    | WEEKOFYEAR
    ;

// https://msdn.microsoft.com/en-us/library/ms188074.aspx
// Spaces are allowed for comparison operators.
comparison_operator
    : '=' | '>' | '<' | '<' '=' | '>' '=' | '<' '>' | '!' '=' | '!' '>' | '!' '<'
    ;

assignment_operator
    : '+=' | '-=' | '*=' | '/=' | '%=' | '&=' | '^=' | '|='
    ;
