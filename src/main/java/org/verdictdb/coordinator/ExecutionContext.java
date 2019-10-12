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

import static org.verdictdb.coordinator.VerdictSingleResultFromListData.createWithSingleColumn;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.verdictdb.VerdictResultStream;
import org.verdictdb.VerdictSingleResult;
import org.verdictdb.commons.DataTypeConverter;
import org.verdictdb.commons.VerdictDBLogger;
import org.verdictdb.commons.VerdictOption;
import org.verdictdb.connection.CachedDbmsConnection;
import org.verdictdb.connection.DbmsConnection;
import org.verdictdb.connection.MetaDataProvider;
import org.verdictdb.connection.StaticMetaData;
import org.verdictdb.core.resulthandler.ExecutionResultReader;
import org.verdictdb.core.scrambling.FastConvergeScramblingMethod;
import org.verdictdb.core.scrambling.HashScramblingMethod;
import org.verdictdb.core.scrambling.ScrambleMeta;
import org.verdictdb.core.scrambling.ScrambleMetaSet;
import org.verdictdb.core.scrambling.ScramblingMethod;
import org.verdictdb.core.scrambling.UniformScramblingMethod;
import org.verdictdb.core.sqlobject.AbstractRelation;
import org.verdictdb.core.sqlobject.BaseTable;
import org.verdictdb.core.sqlobject.ColumnOp;
import org.verdictdb.core.sqlobject.CreateScrambleQuery;
import org.verdictdb.core.sqlobject.JoinTable;
import org.verdictdb.core.sqlobject.SelectQuery;
import org.verdictdb.core.sqlobject.SubqueryColumn;
import org.verdictdb.core.sqlobject.UnnamedColumn;
import org.verdictdb.exception.VerdictDBDbmsException;
import org.verdictdb.exception.VerdictDBException;
import org.verdictdb.exception.VerdictDBTypeException;
import org.verdictdb.metastore.CachedScrambleMetaStore;
import org.verdictdb.metastore.ScrambleMetaStore;
import org.verdictdb.metastore.VerdictMetaStore;
import org.verdictdb.parser.VerdictSQLParser;
import org.verdictdb.parser.VerdictSQLParser.IdContext;
import org.verdictdb.parser.VerdictSQLParserBaseVisitor;
import org.verdictdb.sqlreader.CondGen;
import org.verdictdb.sqlreader.NonValidatingSQLParser;
import org.verdictdb.sqlreader.RelationGen;
import org.verdictdb.sqlreader.RelationStandardizer;
import org.verdictdb.sqlsyntax.SqlSyntax;

/**
 * Stores the context for a single query execution. Includes both scrambling query and select query.
 *
 * @author Yongjoo Park
 */
public class ExecutionContext {

  private DbmsConnection conn;

  private VerdictMetaStore metaStore;

  private QueryContext queryContext;

  private Coordinator runningCoordinator = null;

  private final long serialNumber;

  private final VerdictDBLogger log = VerdictDBLogger.getLogger(getClass());

  private VerdictOption options;

  public enum QueryType {
    select,
    scrambling,
    insert_scramble,
    drop_scramble,
    drop_all_scrambles,
    set_default_schema,
    unknown,
    show_databases,
    show_tables,
    show_scrambles,
    describe_table
  }

  /**
   * @param conn DbmsConnection
   * @param contextId parent's context id
   * @param serialNumber serial number of this ExecutionContext
   * @param options
   */
  public ExecutionContext(
      DbmsConnection conn,
      VerdictMetaStore metaStore,
      String contextId,
      long serialNumber,
      VerdictOption options) {
    this.conn = conn;
    this.metaStore = metaStore;
    this.serialNumber = serialNumber;
    this.queryContext = new QueryContext(contextId, serialNumber);
    this.options = options;
  }

  public long getExecutionContextSerialNumber() {
    return serialNumber;
  }

  /**
   * Check whether given sql contains 'bypass' keyword at the beginning
   *
   * @param sql original sql
   * @return without 'bypass' keyword if the original sql begins with it. null otherwise.
   */
  private String checkBypass(String sql) {
    if (sql.trim().toLowerCase().startsWith("bypass")) {
      return sql.trim().substring(6);
    }
    return null;
  }

  private VerdictSingleResult executeAsIs(String sql) throws VerdictDBDbmsException {
    return new VerdictSingleResultFromDbmsQueryResult(conn.execute(sql));
  }

  public VerdictSingleResult sql(String query) throws VerdictDBException {
    return this.sql(query, true);
  }

  public VerdictSingleResult sql(String query, boolean getResult) throws VerdictDBException {
    String bypassSql = checkBypass(query);
    if (bypassSql != null) {
      return executeAsIs(bypassSql);
    }

    QueryType queryType = identifyQueryType(query);

    if ((queryType != QueryType.select
            && queryType != QueryType.show_databases
            && queryType != QueryType.show_tables
            && queryType != QueryType.describe_table
            && queryType != QueryType.show_scrambles)
        && getResult) {
      throw new VerdictDBException(
          "Can not issue data manipulation statements with executeQuery().");
    }

    if (queryType.equals(QueryType.select)) {
      log.debug("Query type: select");
      return sqlSelectQuery(query, getResult, false);
    }

    // for other types of queries, we invalidate cached metadata for expected data
    // manipulations
    if (conn instanceof CachedDbmsConnection) {
      ((CachedDbmsConnection) conn).clearCache();
    }

    if (queryType.equals(QueryType.scrambling)) {
      log.debug("Query type: scrambling");

      CreateScrambleQuery scrambleQuery = generateScrambleQuery(query);

      List<String> existingPartitionColumns =
          conn.getPartitionColumns(
              scrambleQuery.getOriginalSchema(), scrambleQuery.getOriginalTable());

      // add existing partition columns
      scrambleQuery.setExistingPartitionColumns(existingPartitionColumns);

      // checks the validity; throws an exception if not.
      scrambleQuery.checkIfSupported(conn.getSyntax());

      ScramblingCoordinator scrambler =
          new ScramblingCoordinator(
              conn,
              scrambleQuery.getNewSchema(),
              options.getVerdictTempSchemaName(),
              scrambleQuery.getBlockSize(),
              scrambleQuery.getExistingPartitionColumns());

      // store this metadata to our own metadata db.
      ScrambleMeta meta = scrambler.scramble(scrambleQuery);

      // Add metadata to metastore
      ScrambleMetaStore metaStore = new ScrambleMetaStore(conn, options);
      metaStore.addToStore(meta);
      refreshScrambleMetaStore();
      return null;

    } else if (queryType.equals(QueryType.insert_scramble)) {
      log.debug("Query type: insert_scramble");

      ScrambleMetaStore metaStore = new ScrambleMetaStore(conn, options);
      CreateScrambleQuery scrambleQuery = generateInsertScrambleQuery(query, metaStore);

      ScrambleMeta existingScrambleMeta =
          metaStore.retrieveExistingScramble(
              scrambleQuery.getNewSchema(), scrambleQuery.getNewTable());

      if (existingScrambleMeta == null) {
        throw new VerdictDBException(
            String.format(
                "A scramble '%s.%s' does not exist",
                scrambleQuery.getNewSchema(), scrambleQuery.getNewTable()));
      }

      ScramblingMethod scrambleMethod = null;
      if (existingScrambleMeta.getScramblingMethod() == null) {
        log.warn(
            String.format(
                "Scrambling method information on the scramble '%s.%s' does not exist.",
                scrambleQuery.getNewSchema(), scrambleQuery.getNewTable()));
        log.warn(
            String.format(
                "Using other existing metadata on the scramble '%s.%s' to "
                    + "perform scramble appending.",
                scrambleQuery.getNewSchema(), scrambleQuery.getNewTable()));

        String methodName = existingScrambleMeta.getMethod();
        if (methodName.equalsIgnoreCase("uniform")) {
          scrambleMethod =
              new UniformScramblingMethod(existingScrambleMeta.getCumulativeDistributionForTier());
        } else if (methodName.equalsIgnoreCase("hash")) {
          scrambleMethod =
              new HashScramblingMethod(
                  existingScrambleMeta.getCumulativeDistributionForTier(),
                  existingScrambleMeta.getHashColumn());
        } else if (methodName.equalsIgnoreCase("fastconverge")) {
          scrambleMethod =
              new FastConvergeScramblingMethod(
                  existingScrambleMeta.getCumulativeDistributionForTier(),
                  existingScrambleMeta.getHashColumn());
        }
      } else {
        scrambleMethod = existingScrambleMeta.getScramblingMethod();
      }

      if (scrambleMethod == null) {
        throw new VerdictDBException(
            String.format(
                "Could not determine scrambling method for '%s.%s'.",
                scrambleQuery.getNewSchema(), scrambleQuery.getNewTable()));
      }

      // set scrambleQuery for appending data
      scrambleQuery.setOriginalSchema(existingScrambleMeta.getOriginalSchemaName());
      scrambleQuery.setOriginalTable(existingScrambleMeta.getOriginalTableName());
      scrambleQuery.setMethod(existingScrambleMeta.getMethod());
      scrambleQuery.setHashColumnName(existingScrambleMeta.getHashColumn());
      scrambleQuery.setScramblingMethod(scrambleMethod);

      ScramblingCoordinator scrambler =
          new ScramblingCoordinator(
              conn, scrambleQuery.getNewSchema(), options.getVerdictTempSchemaName());

      // append new scramble
      scrambler.appendScramble(scrambleQuery);
      return null;
    } else if (queryType.equals(QueryType.drop_scramble)) {
      log.debug("Query type: drop_scramble");

      ScrambleMetaStore metaStore = new ScrambleMetaStore(conn, options);
      Pair<BaseTable, BaseTable> tablePair = getTablePairForDropScramble(query);
      metaStore.dropScrambleTable(tablePair.getLeft(), tablePair.getRight());
      return null;

    } else if (queryType.equals(QueryType.drop_all_scrambles)) {
      log.debug("Query type: drop_all_scrambles");

      ScrambleMetaStore metaStore = new ScrambleMetaStore(conn, options);
      BaseTable table = getTableForDropAllScramble(query);
      metaStore.dropAllScrambleTable(table);
      return null;

    } else if (queryType.equals(QueryType.show_scrambles)) {
      log.debug("Query type: show_scrambles");

      ScrambleMetaStore metaStore = new ScrambleMetaStore(conn, options);
      return metaStore.showScrambles();

    } else if (queryType.equals(QueryType.set_default_schema)) {
      log.debug("Query type: set_default_schema");
      updateDefaultSchemaFromQuery(query);
      return null;

    } else if (queryType.equals(QueryType.show_databases)) {
      log.debug("Query type: show_databases");
      return generateShowSchemaResultFromQuery();

    } else if (queryType.equals(QueryType.show_tables)) {
      log.debug("Query type: show_tables");
      return generateShowTablesResultFromQuery(query);

    } else if (queryType.equals(QueryType.describe_table)) {
      log.debug("Query type: describe_table");
      return generateDescribeTableResultFromQuery(query);

    } else {
      throw new VerdictDBTypeException("Unexpected type of query: " + query);
    }
  }

  private VerdictSingleResult sqlSelectQuery(
      String query, boolean getResult, boolean runOriginalQuery
  )
      throws VerdictDBException {
    SelectQuery selectQuery = standardizeQuery(query);
    VerdictResultStream stream = streamSelectQuery(selectQuery, runOriginalQuery);

    if (stream == null) {
      return null;
    }
    QueryResultAccuracyEstimator accEst =
      new QueryResultAccuracyEstimatorFromDifference(selectQuery);

    ShouldProcessWithOriginalDecider runWithOrigDecider = 
      new ShouldProcessWithOriginalDecider(selectQuery, metaStore.retrieve());

    boolean haveCleanedUp = false;

    try {
      while (stream.hasNext()) {
        VerdictSingleResult rs = stream.next();
        accEst.add(rs);
        if (accEst.isLastResultAccurate()) {
          return rs;
        }
        if (runWithOrigDecider.shouldRunOriginal(rs)) {
          stream.close();
          abort();

          haveCleanedUp = true;

          return sqlSelectQuery(query, getResult, true);
        }
      }
      // return the last result otherwise
      return accEst.getAnswers().get(accEst.getAnswerCount() - 1);
    } catch (RuntimeException e) {
      throw e;
    } finally {
      if (!haveCleanedUp) {
        stream.close();
        abort();
      }
    }

    //    SelectQuery selectQuery = standardizeQuery(query);
    //    return streamSelectQuery(selectQuery);

  }

  // not used for now due to instability (i.e., often fails to achieve the goal).
  private void abortInParallel(VerdictResultStream stream) {
    Thread task = new Thread(new ConcurrentAborter(stream));
    task.start();
  }

  class ConcurrentAborter implements Runnable {

    VerdictResultStream stream;

    public ConcurrentAborter(VerdictResultStream stream) {
      this.stream = stream;
    }

    @Override
    public void run() {
      stream.close();
      abort();
    }
  }

  public VerdictResultStream streamsql(String query) throws VerdictDBException {
    // determines the type of the given query and forward it to an appropriate coordinator.
    QueryType queryType = identifyQueryType(query);

    if (!queryType.equals(QueryType.select)) {
      throw new VerdictDBTypeException("Only a select query can be issued to streamsql().");
    }

    SelectQuery selectQuery = standardizeQuery(query);
    return streamSelectQuery(selectQuery, false);
  }

  /**
   * Returns a stream of answers for the select query .
   *
   * @param selectQuery Already standardized select query.
   * @return
   * @throws VerdictDBException
   */
  private VerdictResultStream streamSelectQuery(
    SelectQuery selectQuery, boolean runOriginalQuery
  ) throws VerdictDBException {
    //    selectQuery = standardizeSelectQuery(selectQuery, conn);

    ScrambleMetaSet metaset = metaStore.retrieve();
    SelectQueryCoordinator coordinator = new SelectQueryCoordinator(conn, metaset, options);
    runningCoordinator = null;

    ExecutionResultReader reader = coordinator.process(
      selectQuery, queryContext, runOriginalQuery
    );
    if (coordinator.getLastQuery() != null) {
      // this means there are scrambles for the query so that
      // we need to abort the coordinator at the end.
      runningCoordinator = coordinator;
    }
    VerdictResultStream stream = new VerdictResultStreamFromExecutionResultReader(reader);
    return stream;
  }

  /**
   * Standardizes a query string into a select query object.
   *
   * @param query
   * @return
   * @throws VerdictDBException
   */
  private SelectQuery standardizeQuery(String query) throws VerdictDBException {
    return standardizeQuery(query, conn);
  }

  static SelectQuery standardizeQuery(String query, DbmsConnection conn) throws VerdictDBException {
    SelectQuery selectQuery = NonValidatingSQLParser.toSelectQuery(query);
    SelectQuery standardized = standardizeSelectQuery(selectQuery, conn);
    return standardized;
  }

  /**
   * Standardizes a parsed query.
   *
   * @param selectQuery
   * @return
   * @throws VerdictDBException
   */
  static SelectQuery standardizeSelectQuery(SelectQuery selectQuery, DbmsConnection conn)
      throws VerdictDBException {
    //    if (selectQuery.isStandardized()) {
    //      throw new VerdictDBValueException("The query has already been standardized.");
    //    }

    RelationStandardizer.resetItemID();
    SqlSyntax syntax = conn.getSyntax();
    MetaDataProvider metaData = createMetaDataFor(selectQuery, conn);
    selectQuery = RelationStandardizer.standardizeSelectQuery(selectQuery, metaData, syntax);
    return selectQuery;
  }

  private void refreshScrambleMetaStore() {
    // no type check was added to make it fail if non-cached metastore is used.
    ((CachedScrambleMetaStore) this.metaStore).refreshCache();
  }

  private Pair<BaseTable, BaseTable> getTablePairForDropScramble(String query) {
    VerdictSQLParser parser = NonValidatingSQLParser.parserOf(query);
    VerdictSQLParserBaseVisitor<Pair<BaseTable, BaseTable>> visitor =
        new VerdictSQLParserBaseVisitor<Pair<BaseTable, BaseTable>>() {
          @Override
          public Pair<BaseTable, BaseTable> visitDrop_scramble_statement(
              VerdictSQLParser.Drop_scramble_statementContext ctx) {
            RelationGen g = new RelationGen();
            BaseTable originalTable =
                (ctx.original_table != null) ? (BaseTable) g.visit(ctx.original_table) : null;
            BaseTable scrambleTable = (BaseTable) g.visit(ctx.scrambled_table);
            return ImmutablePair.of(originalTable, scrambleTable);
          }
        };
    return visitor.visit(parser.drop_scramble_statement());
  }

  private BaseTable getTableForDropAllScramble(String query) {
    VerdictSQLParser parser = NonValidatingSQLParser.parserOf(query);
    VerdictSQLParserBaseVisitor<BaseTable> visitor =
        new VerdictSQLParserBaseVisitor<BaseTable>() {
          @Override
          public BaseTable visitDrop_all_scrambles_statement(
              VerdictSQLParser.Drop_all_scrambles_statementContext ctx) {
            RelationGen g = new RelationGen();
            return (BaseTable) g.visit(ctx.original_table);
          }
        };
    return visitor.visit(parser.drop_all_scrambles_statement());
  }

  private String stripQuote(String expr) {
    if (expr == null) {
      return null;
    }
    return expr.replace("\"", "").replace("`", "").replace("'", "");
  }

  private CreateScrambleQuery generateInsertScrambleQuery(String query, ScrambleMetaStore store) {
    VerdictSQLParser parser = NonValidatingSQLParser.parserOf(query);

    VerdictSQLParserBaseVisitor<CreateScrambleQuery> visitor =
        new VerdictSQLParserBaseVisitor<CreateScrambleQuery>() {
          @Override
          public CreateScrambleQuery visitInsert_scramble_statement(
              VerdictSQLParser.Insert_scramble_statementContext ctx) {
            CreateScrambleQuery scrambleQuery = new CreateScrambleQuery();
            RelationGen g = new RelationGen();
            CondGen cond = new CondGen();
            BaseTable scrambleTable = (BaseTable) g.visit(ctx.scrambled_table);
            UnnamedColumn where = cond.visit(ctx.where);

            scrambleQuery.setNewSchema(scrambleTable.getSchemaName());
            scrambleQuery.setNewTable(scrambleTable.getTableName());
            scrambleQuery.setWhere(where);

            return scrambleQuery;
          }
        };

    CreateScrambleQuery scrambleQuery = visitor.visit(parser.insert_scramble_statement());
    return scrambleQuery;
  }

  private CreateScrambleQuery generateScrambleQuery(String query) {

    VerdictSQLParser parser = NonValidatingSQLParser.parserOf(query);
    VerdictSQLParserBaseVisitor<CreateScrambleQuery> visitor =
        new VerdictSQLParserBaseVisitor<CreateScrambleQuery>() {
          @Override
          public CreateScrambleQuery visitCreate_scramble_statement(
              VerdictSQLParser.Create_scramble_statementContext ctx) {
            RelationGen g = new RelationGen();
            BaseTable originalTable = (BaseTable) g.visit(ctx.original_table);
            BaseTable scrambleTable = (BaseTable) g.visit(ctx.scrambled_table);
            String method = (ctx.method == null) ? "uniform" : stripQuote(ctx.method.getText());
            double percent =
                (ctx.percent == null) ? 1.0 : Double.parseDouble(ctx.percent.getText());
            long blocksize =
                (ctx.blocksize == null)
                    ? (long) conn.getSyntax().getRecommendedblockSize()
                    : Long.parseLong(ctx.blocksize.getText());
            String hashColumnName =
                (ctx.hash_column == null) ? null : stripQuote(ctx.hash_column.getText());
            CondGen cond = new CondGen();
            UnnamedColumn where = (ctx.where == null ? null : cond.visit(ctx.where));

            CreateScrambleQuery query =
                new CreateScrambleQuery(
                    scrambleTable.getSchemaName(),
                    scrambleTable.getTableName(),
                    originalTable.getSchemaName(),
                    originalTable.getTableName(),
                    method,
                    percent,
                    blocksize,
                    hashColumnName,
                    where);
            if (ctx.IF() != null) query.setIfNotExists(true);
            return query;
          }
        };

    CreateScrambleQuery scrambleQuery = visitor.visit(parser.create_scramble_statement());
    return scrambleQuery;
  }

  private VerdictSingleResult generateShowSchemaResultFromQuery() throws VerdictDBException {
    List<String> header = Arrays.asList("schema");
    List<String> rows = conn.getSchemas();
    VerdictSingleResultFromListData result = createWithSingleColumn(header, (List) rows);
    return result;
  }

  private VerdictSingleResult generateShowTablesResultFromQuery(String query)
      throws VerdictDBException {
    VerdictSQLParser parser = NonValidatingSQLParser.parserOf(query);
    IdContext schemaCtx = parser.show_tables_statement().schema;
    String schema = (schemaCtx == null) ? conn.getDefaultSchema() : schemaCtx.getText();
    List<String> header = Arrays.asList("table");
    List<String> rows = conn.getTables(schema);
    VerdictSingleResultFromListData result = createWithSingleColumn(header, (List) rows);
    return result;
  }

  private VerdictSingleResult generateDescribeTableResultFromQuery(String query)
      throws VerdictDBException {
    VerdictSQLParser parser = NonValidatingSQLParser.parserOf(query);
    VerdictSQLParserBaseVisitor<Pair<String, String>> visitor =
        new VerdictSQLParserBaseVisitor<Pair<String, String>>() {
          @Override
          public Pair<String, String> visitDescribe_table_statement(
              VerdictSQLParser.Describe_table_statementContext ctx) {
            IdContext schemaCtx = ctx.table.schema;
            String table = ctx.table.table.getText();
            String schema = (schemaCtx == null) ? conn.getDefaultSchema() : schemaCtx.getText();
            //        if (schema == null) {
            //          schema = conn.getDefaultSchema();
            //        }
            return new ImmutablePair<>(schema, table);
          }
        };
    Pair<String, String> t = visitor.visit(parser.verdict_statement());
    String table = t.getRight();
    String schema = t.getLeft();
    if (schema == null) {
      schema = conn.getDefaultSchema();
    }
    List<Pair<String, String>> columnInfo = conn.getColumns(schema, table);
    List<List<String>> newColumnInfo = new ArrayList<>();
    for (Pair<String, String> pair : columnInfo) {
      newColumnInfo.add(Arrays.asList(pair.getLeft(), pair.getRight()));
    }
    List<String> header = Arrays.asList("column name", "column type");
    VerdictSingleResultFromListData result =
        new VerdictSingleResultFromListData(header, (List) newColumnInfo);
    return result;
  }

  private void updateDefaultSchemaFromQuery(String query) throws VerdictDBDbmsException {
    VerdictSQLParser parser = NonValidatingSQLParser.parserOf(query);
    String schema = parser.use_statement().database.getText();
    conn.setDefaultSchema(schema);
  }

  public void abort() {
    log.trace("Aborts an ExecutionContext: " + this);
    if (runningCoordinator != null) {
      Coordinator c = runningCoordinator;
      runningCoordinator = null;
      c.abort();
    }
  }

  /**
   * Terminates existing threads. The created database tables may still exist for successive uses.
   *
   * <p>This method also removes all temporary tables created by this ExecutionContext.
   */
  public void terminate() {
    abort();

    try {
      String schema = options.getVerdictTempSchemaName();
      String tempTablePrefix =
          String.format(
              "%s_%s_%d",
              VerdictOption.getVerdictTempTablePrefix(),
              queryContext.getVerdictContextId(),
              this.serialNumber);
      List<String> tempTableList = new ArrayList<>();

      List<String> allTempTables = null;
      if (conn instanceof CachedDbmsConnection) {
        allTempTables = ((CachedDbmsConnection) conn).getTablesWithoutCaching(schema);
      } else {
        allTempTables = conn.getTables(schema);
      }
      for (String tableName : allTempTables) {
        if (tableName.startsWith(tempTablePrefix)) {
          tempTableList.add(tableName);
        }
      }

      for (String tempTable : tempTableList) {
        conn.execute(String.format("DROP TABLE IF EXISTS %s.%s", schema, tempTable));
      }
    } catch (VerdictDBDbmsException e) {
      e.printStackTrace();
    }
  }

  static MetaDataProvider createMetaDataFor(SelectQuery relation, DbmsConnection conn)
      throws VerdictDBDbmsException {
    StaticMetaData meta = new StaticMetaData();
    String defaultSchema = conn.getDefaultSchema();
    meta.setDefaultSchema(defaultSchema);

    // Extract all tables that appeared in the query
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
          for (AbstractRelation join : ((JoinTable) t).getJoinList()) {
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
          } else if (col instanceof SubqueryColumn) {
            queries.add(((SubqueryColumn) col).getSubquery());
          }
        }
      }
    }

    // Get table info from cached meta
    for (BaseTable t : tables) {
      List<Pair<String, String>> columns;
      StaticMetaData.TableInfo tableInfo;

      if (t.getSchemaName() == null) {
        columns = conn.getColumns(defaultSchema, t.getTableName());
        tableInfo = new StaticMetaData.TableInfo(defaultSchema, t.getTableName());
      } else {
        columns = conn.getColumns(t.getCatalogName(), t.getSchemaName(), t.getTableName());
        tableInfo = new StaticMetaData.TableInfo(t.getSchemaName(), t.getTableName());
      }
      List<Pair<String, Integer>> colInfo = new ArrayList<>();
      for (Pair<String, String> col : columns) {
        colInfo.add(
            new ImmutablePair<>(
                col.getLeft(), DataTypeConverter.typeInt(col.getRight().toLowerCase())));
      }
      meta.addTableData(tableInfo, colInfo);
    }

    return meta;
  }

  MetaDataProvider createMetaDataFor(SelectQuery relation) throws VerdictDBException {
    return createMetaDataFor(relation, conn);
  }

  public static QueryType identifyQueryType(String query) {
    VerdictSQLParser parser = NonValidatingSQLParser.parserOf(query);

    VerdictSQLParserBaseVisitor<QueryType> visitor =
        new VerdictSQLParserBaseVisitor<QueryType>() {

          @Override
          public QueryType visitSelect_statement(VerdictSQLParser.Select_statementContext ctx) {
            return QueryType.select;
          }

          @Override
          public QueryType visitInsert_scramble_statement(
              VerdictSQLParser.Insert_scramble_statementContext ctx) {
            return QueryType.insert_scramble;
          }

          @Override
          public QueryType visitCreate_scramble_statement(
              VerdictSQLParser.Create_scramble_statementContext ctx) {
            return QueryType.scrambling;
          }

          @Override
          public QueryType visitDrop_scramble_statement(
              VerdictSQLParser.Drop_scramble_statementContext ctx) {
            return QueryType.drop_scramble;
          }

          @Override
          public QueryType visitDrop_all_scrambles_statement(
              VerdictSQLParser.Drop_all_scrambles_statementContext ctx) {
            return QueryType.drop_all_scrambles;
          }

          @Override
          public QueryType visitShow_scrambles_statement(
              VerdictSQLParser.Show_scrambles_statementContext ctx) {
            return QueryType.show_scrambles;
          }

          @Override
          public QueryType visitUse_statement(VerdictSQLParser.Use_statementContext ctx) {
            return QueryType.set_default_schema;
          }

          @Override
          public QueryType visitShow_databases_statement(
              VerdictSQLParser.Show_databases_statementContext ctx) {
            return QueryType.show_databases;
          }

          @Override
          public QueryType visitShow_tables_statement(
              VerdictSQLParser.Show_tables_statementContext ctx) {
            return QueryType.show_tables;
          }

          @Override
          public QueryType visitDescribe_table_statement(
              VerdictSQLParser.Describe_table_statementContext ctx) {
            return QueryType.describe_table;
          }
        };

    QueryType type = visitor.visit(parser.verdict_statement());
    if (type == null) {
      type = QueryType.unknown;
    }
    return type;
  }
}
