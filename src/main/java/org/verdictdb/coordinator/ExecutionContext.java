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

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.verdictdb.VerdictContext;
import org.verdictdb.connection.DbmsQueryResult;
import org.verdictdb.core.resulthandler.ExecutionResultReader;
import org.verdictdb.exception.VerdictDBException;
import org.verdictdb.exception.VerdictDBTypeException;
import org.verdictdb.parser.VerdictSQLParser;
import org.verdictdb.parser.VerdictSQLParserBaseVisitor;
import org.verdictdb.sqlreader.NonValidatingSQLParser;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Stores the context for a single query execution. Includes both scrambling query and select query.
 *
 * @author Yongjoo Park
 */
public class ExecutionContext {

  private VerdictContext context;

  private final long serialNumber;

  private enum QueryType {
    select,
    scrambling,
    set_default_schema,
    unknown,
    show_databases,
    show_tables,
    describe_table
  }

  /**
   * @param context Parent context
   * @param contextId
   */
  public ExecutionContext(VerdictContext context, long serialNumber) {
    this.context = context;
    this.serialNumber = serialNumber;
  }

  public long getExecutionContextSerialNumber() {
    return serialNumber;
  }

  public VerdictSingleResult sql(String query) throws VerdictDBException {
    VerdictResultStream stream = streamsql(query);
    if (stream == null) {
      return null;
    }

    VerdictSingleResult result = stream.next();
    stream.close();
    return result;
  }

  public VerdictResultStream streamsql(String query) throws VerdictDBException {
    // determines the type of the given query and forward it to an appropriate coordinator.

    QueryType queryType = identifyQueryType(query);

    if (queryType.equals(QueryType.select)) {
      SelectQueryCoordinator coordinator =
          new SelectQueryCoordinator(context.getCopiedConnection());
      ExecutionResultReader reader = coordinator.process(query);
      VerdictResultStream stream = new VerdictResultStreamFromExecutionResultReader(reader, this);
      return stream;
    } else if (queryType.equals(QueryType.scrambling)) {
      ScramblingCoordinator coordinator = new ScramblingCoordinator(context.getCopiedConnection());
      return null;
    } else if (queryType.equals(QueryType.set_default_schema)) {
      updateDefaultSchemaFromQuery(query);
      return null;
    } else if (queryType.equals(QueryType.show_databases)) {
      return generateShowSchemaResultFromQuery();
    } else if (queryType.equals(QueryType.show_tables)) {
      return generateShowTablesResultFromQuery(query);
    } else if (queryType.equals(QueryType.describe_table)) {
      return generateDesribeTableResultFromQuery(query);
    } else {
      throw new VerdictDBTypeException("Unexpected type of query: " + query);
    }
  }


  private VerdictResultStream generateShowSchemaResultFromQuery() throws VerdictDBException {
    VerdictSingleResultFromListData result = new VerdictSingleResultFromListData((List)context.getConnection().getSchemas());
    return new VerdictResultStreamFromSingleResult(result);
  }

  private VerdictResultStream generateShowTablesResultFromQuery(String query) throws VerdictDBException {
    VerdictSQLParser parser = NonValidatingSQLParser.parserOf(query);
    String schema = parser.show_tables_statement().schema.getText();
    VerdictSingleResultFromListData result = new VerdictSingleResultFromListData(
        (List)context.getConnection().getTables(schema));
    return new VerdictResultStreamFromSingleResult(result);
  }

  private VerdictResultStream generateDesribeTableResultFromQuery(String query) throws VerdictDBException {
    DbmsQueryResult queryResult = context.getConnection().execute(query);
    return new VerdictResultStreamFromSingleResult(new VerdictSingleResultFromDbmsQueryResult(queryResult));
  }

  private void updateDefaultSchemaFromQuery(String query) {
    VerdictSQLParser parser = NonValidatingSQLParser.parserOf(query);
    String schema = parser.use_statement().database.getText();
    context.getConnection().setDefaultSchema(schema);
  }

  /**
   * Terminates existing threads. The created database tables may still exist for successive uses.
   */
  public void terminate() {
    // TODO Auto-generated method stub

  }

  private QueryType identifyQueryType(String query) {
    VerdictSQLParser parser = NonValidatingSQLParser.parserOf(query);
    
    VerdictSQLParserBaseVisitor<QueryType> visitor = new VerdictSQLParserBaseVisitor<QueryType>() {
      
      @Override
      public QueryType visitSelect_statement(VerdictSQLParser.Select_statementContext ctx) {
        return QueryType.select;
      }
      
      @Override
      public QueryType visitCreate_scramble_statement(VerdictSQLParser.Create_scramble_statementContext ctx) {
        return QueryType.scrambling;
      }
      
      @Override
      public QueryType visitUse_statement(VerdictSQLParser.Use_statementContext ctx) {
        return QueryType.set_default_schema;
      }

      @Override
      public QueryType visitShow_databases_statement(VerdictSQLParser.Show_databases_statementContext ctx) {
        return QueryType.show_databases;
      }

      @Override
      public QueryType visitShow_tables_statement(VerdictSQLParser.Show_tables_statementContext ctx) {
        return QueryType.show_tables;
      }

      @Override
      public QueryType visitDescribe_table_statement(VerdictSQLParser.Describe_table_statementContext ctx) {
        return QueryType.describe_table;
      }
    };
    
    QueryType type = visitor.visit(parser.verdict_statement());
    return type;
  }
}
