/*
 *    Copyright 2018 University of Michigan
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package org.verdictdb.coordinator;

import java.util.LinkedList;
import java.util.List;

import org.apache.commons.lang3.tuple.Triple;
import org.verdictdb.commons.VerdictDBLogger;
import org.verdictdb.commons.VerdictOption;
import org.verdictdb.connection.DbmsConnection;
import org.verdictdb.core.execplan.ExecutablePlanRunner;
import org.verdictdb.core.execplan.ExecutionInfoToken;
import org.verdictdb.core.execplan.ExecutionTokenQueue;
import org.verdictdb.core.querying.QueryExecutionPlan;
import org.verdictdb.core.querying.QueryExecutionPlanFactory;
import org.verdictdb.core.querying.ola.AsyncQueryExecutionPlan;
import org.verdictdb.core.querying.simplifier.QueryExecutionPlanSimplifier;
import org.verdictdb.core.resulthandler.ExecutionResultReader;
import org.verdictdb.core.scrambling.ScrambleMetaSet;
import org.verdictdb.core.sqlobject.AbstractRelation;
import org.verdictdb.core.sqlobject.AliasedColumn;
import org.verdictdb.core.sqlobject.BaseColumn;
import org.verdictdb.core.sqlobject.BaseTable;
import org.verdictdb.core.sqlobject.ColumnOp;
import org.verdictdb.core.sqlobject.CreateSchemaQuery;
import org.verdictdb.core.sqlobject.JoinTable;
import org.verdictdb.core.sqlobject.SelectItem;
import org.verdictdb.core.sqlobject.SelectQuery;
import org.verdictdb.core.sqlobject.UnnamedColumn;
import org.verdictdb.exception.VerdictDBException;
import org.verdictdb.exception.VerdictDBValueException;
import org.verdictdb.sqlreader.ScrambleTableReplacer;

public class SelectQueryCoordinator implements Coordinator {

  private ExecutablePlanRunner planRunner;

  DbmsConnection conn;

  ScrambleMetaSet scrambleMetaSet;

  String scratchpadSchema;

  SelectQuery lastQuery;

  VerdictOption options;

  private VerdictDBLogger log = VerdictDBLogger.getLogger(this.getClass());

  public SelectQueryCoordinator(DbmsConnection conn) {
    this(conn, new ScrambleMetaSet(), new VerdictOption());
  }

  public SelectQueryCoordinator(DbmsConnection conn, VerdictOption options) {
    this(conn, new ScrambleMetaSet(), options);
    this.options = options;
  }

  public SelectQueryCoordinator(
      DbmsConnection conn, ScrambleMetaSet scrambleMetaSet, VerdictOption options) {
    this(conn, scrambleMetaSet, options.getVerdictTempSchemaName());
    this.options = options;
  }

  public SelectQueryCoordinator(
      DbmsConnection conn, ScrambleMetaSet scrambleMetaSet, String scratchpadSchema) {
    this.conn = conn;
    this.scrambleMetaSet = scrambleMetaSet;
    this.scratchpadSchema = scratchpadSchema;
  }

  public ScrambleMetaSet getScrambleMetaSet() {
    return scrambleMetaSet;
  }

  public void setScrambleMetaSet(ScrambleMetaSet scrambleMetaSet) {
    this.scrambleMetaSet = scrambleMetaSet;
  }

  public SelectQuery getLastQuery() {
    return lastQuery;
  }
  
  /**
   * This method must be used only for testing. Currently, process(SelectQuery selectQuery)
   * is used instead by ExecutionContext.
   * @param sql
   * @return
   * @throws VerdictDBException
   */
  public ExecutionResultReader process(String sql) throws VerdictDBException {
    SelectQuery selectQuery = ExecutionContext.standardizeQuery(sql, conn);
    return process(selectQuery);
  }
  
  /**
   * The input is assumed to have been standardized.
   */
  public ExecutionResultReader process(SelectQuery selectQuery) throws VerdictDBException {
    return process(selectQuery, null, false);
  }

  public ExecutionResultReader process(
    SelectQuery selectQuery, QueryContext context, boolean useOriginalQuery
  ) throws VerdictDBException {
    if (useOriginalQuery) {
      log.info(
        "Passed USE_ORIGINAL_AFTER threshold -- running raw original query"
      );

      return startOriginalQuery(selectQuery);
    }

    // create scratchpad schema if not exists
    if (!conn.getSchemas().contains(scratchpadSchema)) {
      log.info(
          String.format(
              "The schema for temporary tables (%s) does not exist; so we create it.",
              scratchpadSchema));
      CreateSchemaQuery createSchema = new CreateSchemaQuery(scratchpadSchema);
      conn.execute(createSchema);
    }
    
    // replaces original tables with scrambles if available
    SelectQuery fasterQuery = lookforReplacement2Scrambles(selectQuery);

    lastQuery = null;
    if (fasterQuery == null) {
      // this means there are no scrambles available, we should run it as-is
      log.debug("No scrambles available for the query. We will execute it as-is.");
      return startOriginalQuery(selectQuery);
    }

    // make plan
    // if the plan does not include any aggregates, it will simply be a parsed structure of the
    // original query.
    QueryExecutionPlan plan =
        QueryExecutionPlanFactory.create(scratchpadSchema, scrambleMetaSet, fasterQuery, context);

    // convert it to an asynchronous plan
    // if the plan does not include any aggregates, this operation should not alter the original
    // plan.
    QueryExecutionPlan asyncPlan = AsyncQueryExecutionPlan.create(plan);
    log.debug("Async plan created.");

    // simplify the plan
    //    QueryExecutionPlan simplifiedAsyncPlan = QueryExecutionPlanSimplifier.simplify(asyncPlan);
    QueryExecutionPlanSimplifier.simplify2(asyncPlan);
    log.debug("Plan simplification done.");
    log.trace(asyncPlan.getRoot().getStructure());

    // execute the plan
    planRunner = new ExecutablePlanRunner(conn, asyncPlan);
    ExecutionResultReader reader = planRunner.getResultReader();

    lastQuery = fasterQuery;

    return reader;
  }

  private ExecutionResultReader startOriginalQuery(SelectQuery selectQuery) throws VerdictDBException {
    ExecutionInfoToken token = ExecutionInfoToken.empty();
    ExecutionTokenQueue queue = new ExecutionTokenQueue();
    token.setKeyValue("queryResult", conn.execute(selectQuery));
    queue.add(token);
    queue.add(ExecutionInfoToken.successToken());
    return new ExecutionResultReader(queue);
  }

  @Override
  public void abort() {
    if (planRunner != null) {
      log.debug(String.format("Closes %s.", this.getClass().getSimpleName()));
      planRunner.abort();
      planRunner = null;
    }
  }
  
  /**
   * Ensures that simple aggregates (i.e., sum, count, avg) are associated with uniform scrambles,
   * and that count-distinct aggregates are associated with hash scrambles.
   * 
   * @param query
   * @throws VerdictDBException
   */
  private void ensureScrambleCorrectness(SelectQuery query) throws VerdictDBException {
    ensureScrambleCorrectnessInner(query, null);
  }
  
  private void ensureScrambleCorrectnessInner(SelectQuery query, BaseColumn countDistinctColumn)
      throws VerdictDBException {
    
    Triple<Boolean, Boolean, BaseColumn> inspectionInfo = inspectAggregatesInSelectList(query);
    boolean containAggregateItem = inspectionInfo.getLeft();
    boolean containCountDistinctItem = inspectionInfo.getMiddle();
    countDistinctColumn = inspectionInfo.getRight();
    
    // check from list
    for (AbstractRelation table : query.getFromList()) {
      if (table instanceof BaseTable) {
        String schemaName = ((BaseTable) table).getSchemaName();
        String tableName = ((BaseTable) table).getTableName();
        if (!scrambleMetaSet.isScrambled(schemaName, tableName)) {
          continue;
        }
        String method = scrambleMetaSet.getScramblingMethod(schemaName, tableName);

        if (containAggregateItem) {
          if (!method.equalsIgnoreCase("uniform")
              && !method.equalsIgnoreCase("fastconverge")) {
            throw new VerdictDBValueException(
                "Simple aggregates must be used with a uniform scramble.");
          }
        } else if (containCountDistinctItem) {
          String hashColumn = scrambleMetaSet.getHashColumn(schemaName, tableName);
          if (!method.equalsIgnoreCase("hash")
              || hashColumn == null
              || !hashColumn.equalsIgnoreCase(countDistinctColumn.getColumnName())) {
            throw new VerdictDBValueException(
                "Count distinct of a column must be used with the hash scramble "
                    + "built on that column.");
          }
        }
        
      } else if (table instanceof JoinTable) {
        for (AbstractRelation jointable : ((JoinTable) table).getJoinList()) {
          if (jointable instanceof SelectQuery) {
            ensureQuerySupport((SelectQuery) jointable);
          }
        }
      } else if (table instanceof SelectQuery) {
        ensureScrambleCorrectnessInner((SelectQuery) table, countDistinctColumn);
      }  
    }
  }
  
  /**
   * For the third element, the operand inside the count-distinct, we recursively examine the
   * columnOp until we find count-distinct.
   * 
   * @return (ifContainsSimpleAggregates, ifContainsCountDistinct, countDistinctColumn)
   */
  static public Triple<Boolean, Boolean, BaseColumn> inspectAggregatesInSelectList(SelectQuery query) {
    // check select list
    boolean containAggregatedItem = false;
    boolean containCountDistinctItem = false;
    BaseColumn countDistinctColumn = null;
    
    for (SelectItem selectItem : query.getSelectList()) {
      if (selectItem instanceof AliasedColumn
          && ((AliasedColumn) selectItem).getColumn() instanceof ColumnOp) {
        ColumnOp opcolumn = (ColumnOp) ((AliasedColumn) selectItem).getColumn();
        
        if (opcolumn.isCountDistinctAggregate()) {
          // since the count-distinct may exist inside some other functions,
          // e.g., cast(count(distinct col) as integer), we check the internal operands recursively.
          containCountDistinctItem = true;
          
          List<UnnamedColumn> cols = new LinkedList<>();
          cols.add(opcolumn);
          while (cols.size() > 0) {
            UnnamedColumn col = cols.remove(0);
            if (col instanceof ColumnOp 
                && (((ColumnOp) col).getOpType().equals("countdistinct")
                || ((ColumnOp) col).getOpType().equals("approx_distinct"))) {
              UnnamedColumn operand = ((ColumnOp) col).getOperand();
              if (operand instanceof BaseColumn) {
                countDistinctColumn = (BaseColumn) operand;
              }
            } else if (col instanceof ColumnOp) {
              cols.addAll(((ColumnOp) col).getOperands());
            }
          }
        }
        if (opcolumn.isUniformSampleAggregateColumn()) {
          containAggregatedItem = true;
        }
      }
    }
    
    return Triple.of(containAggregatedItem, containCountDistinctItem, countDistinctColumn);
  }

  /**
   * Ensures that simple aggregates (i.e., sum, count, avg) and count-distinct do not appear
   * together.
   * 
   * @param query Select query
   * @return check if the query contain the syntax that is not supported by VerdictDB
   */
  private void ensureQuerySupport(SelectQuery query) throws VerdictDBException {
    
    Triple<Boolean, Boolean, BaseColumn> inspectionInfo = inspectAggregatesInSelectList(query);
    boolean containAggregatedItem = inspectionInfo.getLeft();
    boolean containCountDistinctItem = inspectionInfo.getMiddle();
    
    if (containAggregatedItem && containCountDistinctItem) {
      throw new VerdictDBException(
          "Count distinct and other aggregate functions cannot appear in the same select list.");
    }
    
    // check from list
    for (AbstractRelation table : query.getFromList()) {
      if (table instanceof SelectQuery) {
        ensureQuerySupport((SelectQuery) table);
      }  else if (table instanceof JoinTable) {
        for (AbstractRelation jointable : ((JoinTable) table).getJoinList()) {
          if (jointable instanceof SelectQuery) {
            ensureQuerySupport((SelectQuery) jointable);
          }
        }
      }
    }

    // also need to check the having clause
    // since we will convert having clause into select list
    if (query.getHaving().isPresent()) {
      UnnamedColumn having = query.getHaving().get();
      if (having instanceof ColumnOp
          && ((ColumnOp) having).isCountDistinctAggregate()) {
        containCountDistinctItem = true;
//        ((ColumnOp) having).replaceAllColumnOpOpType("countdistnct", "approx_distinct");
      }
      if (having instanceof ColumnOp && ((ColumnOp) having).isUniformSampleAggregateColumn()) {
        containAggregatedItem = true;
      }
      if (containAggregatedItem && containCountDistinctItem) {
        throw new VerdictDBException(
            "Count distinct and other aggregate functions cannot appear in the same select list.");
      }
    }
  }

//  /**
//   * This must be used only for testing.
//   * @param query
//   * @return
//   * @throws VerdictDBException
//   */
//  private SelectQuery standardizeQuery(String query) throws VerdictDBException {
//    // parse the query
//    RelationStandardizer.resetItemID();
//    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
//    SelectQuery selectQuery = (SelectQuery) sqlToRelation.toRelation(query);
//    MetaDataProvider metaData = ExecutionContext.createMetaDataFor(selectQuery, conn);
//    RelationStandardizer gen = new RelationStandardizer(metaData, conn.getSyntax());
//    selectQuery = gen.standardize(selectQuery);
//    
//    return selectQuery;
//  }
  
  /**
   * Replaces original tables with corresponding scrambles.
   * @param query
   * @return
   * @throws VerdictDBException
   */
  private SelectQuery lookforReplacement2Scrambles(SelectQuery query) throws VerdictDBException {
 // Ensure that the query does not have unsupported syntax, 
    // such as count distinct with other agg. 
    // Otherwise, it will throw to an exception
    ensureQuerySupport(query);

    // replaces original tables with scrambles
    ScrambleTableReplacer replacer = new ScrambleTableReplacer(scrambleMetaSet);
    int scrambleCount = replacer.replaceQuery(query);
    
    // ensure scramble validity
    // Moreover, if the agg is count-distinct, either the specified scramble is of type
    // 'hash scramble' or there must exist a hash scramble created for the specified (original) 
    // table.
    ensureScrambleCorrectness(query);
    
    // TODO: Replace count-distinct with approx-count-distinct
    
    
    return (scrambleCount == 0) ? null : query;
  }

//  private MetaDataProvider createMetaDataFor(SelectQuery relation) throws VerdictDBException {
//    StaticMetaData meta = new StaticMetaData();
//    String defaultSchema = conn.getDefaultSchema();
//    meta.setDefaultSchema(defaultSchema);
//
//    // Extract all tables appeared in the query
//    HashSet<BaseTable> tables = new HashSet<>();
//    List<SelectQuery> queries = new ArrayList<>();
//    queries.add(relation);
//    while (!queries.isEmpty()) {
//      SelectQuery query = queries.get(0);
//      queries.remove(0);
//      for (AbstractRelation t : query.getFromList()) {
//        if (t instanceof BaseTable) tables.add((BaseTable) t);
//        else if (t instanceof SelectQuery) queries.add((SelectQuery) t);
//        else if (t instanceof JoinTable) {
//          for (AbstractRelation join : ((JoinTable) t).getJoinList()) {
//            if (join instanceof BaseTable) tables.add((BaseTable) join);
//            else if (join instanceof SelectQuery) queries.add((SelectQuery) join);
//          }
//        }
//      }
//      if (query.getFilter().isPresent()) {
//        UnnamedColumn where = query.getFilter().get();
//        List<UnnamedColumn> toCheck = new ArrayList<>();
//        toCheck.add(where);
//        while (!toCheck.isEmpty()) {
//          UnnamedColumn col = toCheck.get(0);
//          toCheck.remove(0);
//          if (col instanceof ColumnOp) {
//            toCheck.addAll(((ColumnOp) col).getOperands());
//          } else if (col instanceof SubqueryColumn) {
//            queries.add(((SubqueryColumn) col).getSubquery());
//          }
//        }
//      }
//    }
//
//    // Get table info from cached meta
//    for (BaseTable t : tables) {
//      List<Pair<String, String>> columns;
//      StaticMetaData.TableInfo tableInfo;
//
//      if (t.getSchemaName() == null) {
//        columns = conn.getColumns(defaultSchema, t.getTableName());
//        tableInfo = new StaticMetaData.TableInfo(defaultSchema, t.getTableName());
//      } else {
//        columns = conn.getColumns(t.getSchemaName(), t.getTableName());
//        tableInfo = new StaticMetaData.TableInfo(t.getSchemaName(), t.getTableName());
//      }
//      List<Pair<String, Integer>> colInfo = new ArrayList<>();
//      for (Pair<String, String> col : columns) {
//        colInfo.add(
//            new ImmutablePair<>(
//                col.getLeft(), DataTypeConverter.typeInt(col.getRight().toLowerCase())));
//      }
//      meta.addTableData(tableInfo, colInfo);
//    }
//
//    return meta;
//  }
}
