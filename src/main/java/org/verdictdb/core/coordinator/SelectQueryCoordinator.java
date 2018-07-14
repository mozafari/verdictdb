package org.verdictdb.core.coordinator;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.verdictdb.core.connection.CachedMetaDataProvider;
import org.verdictdb.core.connection.DataTypeConverter;
import org.verdictdb.core.connection.DbmsConnection;
import org.verdictdb.core.connection.StaticMetaData;
import org.verdictdb.core.execution.ExecutablePlanRunner;
import org.verdictdb.core.querying.QueryExecutionPlan;
import org.verdictdb.core.querying.QueryExecutionPlanSimplifier;
import org.verdictdb.core.querying.ola.AsyncQueryExecutionPlan;
import org.verdictdb.core.resulthandler.ExecutionResultReader;
import org.verdictdb.core.scrambling.ScrambleMetaSet;
import org.verdictdb.core.sqlobject.*;
import org.verdictdb.exception.VerdictDBException;
import org.verdictdb.sqlreader.NonValidatingSQLParser;
import org.verdictdb.sqlreader.RelationStandardizer;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

public class SelectQueryCoordinator {

  DbmsConnection conn;

  ScrambleMetaSet scrambleMetaSet;

  String defaultSchema = "tpch";

  StaticMetaData staticMetaData = new StaticMetaData();

  CachedMetaDataProvider cachedMetaData;

  public SelectQueryCoordinator(DbmsConnection conn) {
    this.conn = conn;
  }

  public SelectQuery standardizeQuery(String query) throws VerdictDBException {
    // parse the query
    RelationStandardizer.resetItemID();
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    SelectQuery relation = (SelectQuery) sqlToRelation.toRelation(query);
    setStaticMetaData(relation);
    RelationStandardizer gen = new RelationStandardizer(staticMetaData);
    relation = gen.standardize(relation);
    return relation;
  }

  public ScrambleMetaSet getScrambleMetaSet() {
    return scrambleMetaSet;
  }

  public void setScrambleMetaSet(ScrambleMetaSet scrambleMetaSet) {
    this.scrambleMetaSet = scrambleMetaSet;
  }

  public StaticMetaData getStaticMetaData() {
    return staticMetaData;
  }

  public void setDefaultSchema(String defaultSchema) {
    this.defaultSchema = defaultSchema;
  }

  public void setStaticMetaData(SelectQuery relation) throws VerdictDBException {
    cachedMetaData = new CachedMetaData(conn);
    staticMetaData.setDefaultSchema(defaultSchema);
    // Extract all tables appeared in the query
    HashSet<BaseTable> tables = new HashSet<>();
    List<SelectQuery> queries = new ArrayList<>();
    queries.add(relation);
    while (!queries.isEmpty()) {
      SelectQuery query = queries.get(0);
      queries.remove(0);
      for (AbstractRelation t : query.getFromList()) {
        if (t instanceof BaseTable) tables.add((BaseTable) t);
        else if (t instanceof SelectQuery) queries.add((SelectQuery) t);
        else if (t instanceof JoinTable) {
          for (AbstractRelation join:((JoinTable) t).getJoinList()) {
            if (join instanceof BaseTable) tables.add((BaseTable) join);
            else if (join instanceof SelectQuery) queries.add((SelectQuery) join);
          }
        }
      }
      if (query.getFilter().isPresent()) {
        UnnamedColumn where = query.getFilter().get();
        List<UnnamedColumn> toCheck = new ArrayList<>();
        toCheck.add(where);
        while (!toCheck.isEmpty()) {
          UnnamedColumn col = toCheck.get(0);
          toCheck.remove(0);
          if (col instanceof ColumnOp) {
            toCheck.addAll(((ColumnOp) col).getOperands());
          }
          else if (col instanceof SubqueryColumn) {
            queries.add(((SubqueryColumn) col).getSubquery());
          }
        }
      }
    }

    // Get table info from cached meta
    for (BaseTable t:tables) {
      List<Pair<String, String>> columns;
      StaticMetaData.TableInfo tableInfo;
      if (t.getSchemaName()==null) {
        columns = cachedMetaData.getColumns(defaultSchema, t.getTableName());
        tableInfo = new StaticMetaData.TableInfo(defaultSchema, t.getTableName());
      }
      else {
        columns = cachedMetaData.getColumns(t.getSchemaName(), t.getTableName());
        tableInfo = new StaticMetaData.TableInfo(t.getSchemaName(), t.getTableName());
      }
      List<Pair<String, Integer>> colInfo = new ArrayList<>();
      for (Pair<String, String> col:columns) {
        colInfo.add(new ImmutablePair<>(col.getLeft(), DataTypeConverter.typeInt(col.getRight().toLowerCase())));
      }
      staticMetaData.addTableData(tableInfo, colInfo);
    }
  }

  public ExecutionResultReader process(String query) throws VerdictDBException {

    SelectQuery selectQuery = standardizeQuery(query);

    // make plan
    // if the plan does not include any aggregates, it will simply be a parsed structure of the original query.
    QueryExecutionPlan plan = new QueryExecutionPlan("verdictdb_temp", scrambleMetaSet, selectQuery);;

    // convert it to an asynchronous plan
    // if the plan does not include any aggregates, this operation should not alter the original plan.
    QueryExecutionPlan asyncPlan = AsyncQueryExecutionPlan.create(plan);;

    // simplify the plan
    QueryExecutionPlan simplifiedAsyncPlan = QueryExecutionPlanSimplifier.simplify(asyncPlan);

    // execute the plan
    ExecutionResultReader reader = ExecutablePlanRunner.getResultReader(conn, simplifiedAsyncPlan);

    return reader;
  }

}
