package org.verdictdb.core.querying;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.sql.DriverManager;
import java.sql.SQLException;

import org.junit.BeforeClass;
import org.junit.Test;
import org.verdictdb.connection.DbmsConnection;
import org.verdictdb.connection.JdbcConnection;
import org.verdictdb.core.querying.ola.AsyncQueryExecutionPlan;
import org.verdictdb.core.querying.simplifier.ConsolidatedExecutionNode;
import org.verdictdb.core.querying.simplifier.QueryExecutionPlanSimplifier;
import org.verdictdb.core.scrambling.ScrambleMetaSet;
import org.verdictdb.core.sqlobject.ColumnOp;
import org.verdictdb.core.sqlobject.SelectQuery;
import org.verdictdb.core.sqlobject.SubqueryColumn;
import org.verdictdb.exception.VerdictDBDbmsException;
import org.verdictdb.exception.VerdictDBException;
import org.verdictdb.exception.VerdictDBValidationException;
import org.verdictdb.sqlreader.NonValidatingSQLParser;
import org.verdictdb.sqlsyntax.H2Syntax;

public class QueryExecutionPlanSimplifierTest {

  static String originalSchema = "originalschema";

  static String originalTable = "originaltable";

  static String newSchema = "newschema";

  static String newTable  = "newtable";

  static int aggblockCount = 2;

  static DbmsConnection conn;
  
  static ScrambleMetaSet scrambleMeta;

  @BeforeClass
  public static void setupDbConnAndScrambledTable() throws SQLException, VerdictDBException {
    final String DB_CONNECTION = "jdbc:h2:mem:plancompresstest;DB_CLOSE_DELAY=-1";
    final String DB_USER = "";
    final String DB_PASSWORD = "";
    conn = new JdbcConnection(DriverManager.getConnection(DB_CONNECTION, DB_USER, DB_PASSWORD), new H2Syntax());
    conn.execute(String.format("CREATE SCHEMA IF NOT EXISTS\"%s\"", originalSchema));
    conn.execute(String.format("CREATE SCHEMA IF NOT EXISTS\"%s\"", newSchema));
    populateData(conn, originalSchema, originalTable);
  }

  static void populateData(DbmsConnection conn, String schemaName, String tableName) 
      throws VerdictDBDbmsException {
    conn.execute(
        String.format("CREATE TABLE \"%s\".\"%s\"(\"id\" int, \"value\" double)", 
            schemaName, tableName));
    for (int i = 0; i < 10; i++) {
      conn.execute(String.format("INSERT INTO \"%s\".\"%s\"(\"id\", \"value\") VALUES(%s, %f)",
          schemaName, tableName, i, (double) i+1));
    }
    
    scrambleMeta = new ScrambleMetaSet();
    scrambleMeta.insertScrambleMetaEntry(
        newSchema, newTable,
        "aggregationBlockColumn", "subsampleColumn", "tierColumn", 3);
  }

  @Test
  public void simpleAggregateVersion2Test() throws VerdictDBValidationException {
    String sql = "select avg(t.value) as a from originalschema.originaltable as t";
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    SelectQuery selectQuery = (SelectQuery) sqlToRelation.toRelation(sql);
    SelectQuery originalQuery = selectQuery.deepcopy();
    QueryExecutionPlan plan = QueryExecutionPlanFactory.create(newSchema, null, selectQuery);

    assertEquals(
        originalQuery,
        ((QueryNodeBase) plan.getRootNode().getExecutableNodeBaseDependent(0))
        .getSelectQuery());

    // after simplification
    QueryExecutionPlanSimplifier.simplify2(plan);

    //    System.out.println(((QueryNodeBase) plan.getRootNode()).getSelectQuery());
    assertEquals(
        originalQuery,
        ((QueryNodeBase) plan.getRootNode()).getSelectQuery());

    // the root must not have any children now.
    assertEquals(0, plan.getRootNode().getSourceCount());

    //    plan.getRootNode().print();
  }

  @Test
  public void NestedAggregateFromVersion2Test() throws VerdictDBValidationException {
    String sql = "select avg(t.value) from (" +
        "select o.value " +
        "from originalschema.originaltable as o " +
        "where o.value > 5) as t;";
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    SelectQuery selectQuery = (SelectQuery) sqlToRelation.toRelation(sql);
    SelectQuery originalQuery = selectQuery.deepcopy();
    QueryExecutionPlan plan = QueryExecutionPlanFactory.create(newSchema, null, selectQuery);

    //    System.out.println(((QueryNodeBase) plan.getRootNode()).getSelectQuery());
    plan.getRootNode().print();

    // after simplification
    QueryExecutionPlanSimplifier.simplify2(plan);

    //      System.out.println(((QueryNodeBase) plan.getRootNode()).getSelectQuery());
    assertEquals(
        originalQuery,
        ((QueryNodeBase) plan.getRootNode()).getSelectQuery());

    plan.getRootNode().print();
  }

  @Test
  public void NestedAggregateFilterVersion2Test() throws VerdictDBValidationException {
    String sql = "select avg(t.value) as a " +
        "from originalschema.originaltable as t " +
        "where t.value > (" +
        "select avg(o.value) as avg_value " +
        "from originalschema.originaltable as o);";
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    SelectQuery selectQuery = (SelectQuery) sqlToRelation.toRelation(sql);
    SelectQuery originalQuery = selectQuery.deepcopy();
    QueryExecutionPlan plan = QueryExecutionPlanFactory.create(newSchema, null, selectQuery);
    plan.getRootNode().print();
  
    //    System.out.println(((QueryNodeBase) plan.getRootNode()).getSelectQuery());
    //    System.out.println(((QueryNodeBase) plan.getRootNode().getExecutableNodeBaseDependent(0))
    //                           .getSelectQuery());
  
    // after simplification
    QueryExecutionPlanSimplifier.simplify2(plan);
  
    //    System.out.println(((QueryNodeBase) plan.getRootNode()).getSelectQuery());
    System.out.println(originalQuery);
  
    assertEquals(
        originalQuery,
        ((QueryNodeBase) plan.getRootNode()).getSelectQuery());
  
    //    plan.getRootNode().print();
  }

  @Test
  public void JoinQueryVersion2Test() throws VerdictDBValidationException {
    String sql = "select avg(t.value) as a " +
        "from (select * from originalschema.originaltable as t0) t1 " +
        "inner join (select price as p from anotherSchema.anotherTable as t2) t3 " +
        "on t1.join_key = t3.join_key";
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    SelectQuery selectQuery = (SelectQuery) sqlToRelation.toRelation(sql);
    SelectQuery originalQuery = selectQuery.deepcopy();
    QueryExecutionPlan plan = QueryExecutionPlanFactory.create(newSchema, null, selectQuery);
    plan.getRootNode().print();
  
    //    System.out.println(((QueryNodeBase) plan.getRootNode()).getSelectQuery());
    //    System.out.println(((QueryNodeBase) plan.getRootNode().getExecutableNodeBaseDependent(0))
    //                           .getSelectQuery());
  
    // after simplification
    QueryExecutionPlanSimplifier.simplify2(plan);
  
    //    System.out.println(((QueryNodeBase) plan.getRootNode()).getSelectQuery());
    System.out.println(originalQuery);
  
    assertEquals(
        originalQuery,
        ((QueryNodeBase) plan.getRootNode()).getSelectQuery());
  
    //    plan.getRootNode().print();
  }
  
  @Test
  public void simpleAsyncAggVersion2Test() throws VerdictDBException {
    String sql = "select avg(t.value) as a from newschema.newtable as t";
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    SelectQuery selectQuery = (SelectQuery) sqlToRelation.toRelation(sql);
    QueryExecutionPlan plan = 
        QueryExecutionPlanFactory.create(newSchema, scrambleMeta, selectQuery);
    QueryExecutionPlan asyncPlan = AsyncQueryExecutionPlan.create(plan);
    
//    asyncPlan.getRoot().print();

    QueryExecutionPlanSimplifier.simplify2(asyncPlan);

    // Test 1: after simplication, the root must be the instanceof DirectRetrievalExecutionNode
    assertTrue(asyncPlan.getRootNode() instanceof ConsolidatedExecutionNode);

    // Test 2: the number of children must be equal to the agg block count. 
    assertEquals(3, asyncPlan.getRootNode().getSourceCount());
  }
  
  @Test
  public void nestedAsyncAggVersion2Test() throws VerdictDBException {
    String sql = "select avg(t.value) from (" +
        "select o.value " +
        "from newschema.newtable as o " +
        "where o.value > 5) as t;";
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    SelectQuery selectQuery = (SelectQuery) sqlToRelation.toRelation(sql);
    QueryExecutionPlan plan = 
        QueryExecutionPlanFactory.create(newSchema, scrambleMeta, selectQuery);
    QueryExecutionPlan asyncPlan = AsyncQueryExecutionPlan.create(plan);
    
    QueryExecutionPlanSimplifier.simplify2(asyncPlan);
    asyncPlan.getRoot().print();

    // Test 1: after simplifying, the root must be the instance of DirectRetrievalExecutionNode.
    assertTrue(asyncPlan.getRootNode() instanceof ConsolidatedExecutionNode);

    // Test 2: the number of children must be equal to the agg block count. 
    assertEquals(3, asyncPlan.getRootNode().getSourceCount());
    
    // Test 3: the individual aggregate node must be consolidated into DirectRetrievalExecutionNode
    ExecutableNodeBase firstAgg = asyncPlan.getRootNode().getSources().get(0);
    assertEquals(ConsolidatedExecutionNode.class, firstAgg.getClass());
  }

  @Test
  public void simpleAggregateTest() throws VerdictDBException {
    String sql = "select avg(t.value) as a from originalschema.originaltable as t;";
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    SelectQuery selectQuery = (SelectQuery) sqlToRelation.toRelation(sql);
    QueryExecutionPlan queryExecutionPlan = QueryExecutionPlanFactory.create(newSchema, null, selectQuery);
    ExecutableNodeBase copy = queryExecutionPlan.root.deepcopy();
    QueryExecutionPlan plan = QueryExecutionPlanSimplifier.simplify(queryExecutionPlan);
    assertEquals(0, plan.root.getDependentNodeCount());
    selectQuery.setAliasName("t");
    assertEquals(selectQuery, ((QueryNodeBase) plan.root).selectQuery.getFromList().get(0));
    ((QueryNodeBase) copy.getExecutableNodeBaseDependent(0)).selectQuery.setAliasName("t");
    assertEquals(
        ((QueryNodeBase) copy.getExecutableNodeBaseDependent(0)).selectQuery, 
        ((QueryNodeBase) plan.root).selectQuery.getFromList().get(0));

    // queryExecutionPlan.root.execute(conn);
  }

  @Test
  public void NestedAggregateFromTest() throws VerdictDBException {
    String sql = "select avg(t.value) from (select o.value from originalschema.originaltable as o where o.value>5) as t;";
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    SelectQuery selectQuery = (SelectQuery) sqlToRelation.toRelation(sql);
    QueryExecutionPlan queryExecutionPlan = QueryExecutionPlanFactory.create(newSchema, null, selectQuery);
    ExecutableNodeBase copy = queryExecutionPlan.root.getExecutableNodeBaseDependent(0).getExecutableNodeBaseDependent(0).deepcopy();
    QueryExecutionPlan plan = QueryExecutionPlanSimplifier.simplify(queryExecutionPlan);
    assertEquals(0, plan.root.getDependentNodeCount());
    assertEquals(
        ((QueryNodeBase) copy).selectQuery,
        ((SelectQuery)
            ((QueryNodeBase) plan.root).selectQuery.getFromList().get(0))
        .getFromList().get(0));
    // queryExecutionPlan.root.execute(conn);
  }

  @Test
  public void NestedAggregateFilterTest() throws VerdictDBException {
    String sql = "select avg(t.value) as a from originalschema.originaltable as t where t.value > " +
        "(select avg(o.value) as avg_value from originalschema.originaltable as o);";
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    SelectQuery selectQuery = (SelectQuery) sqlToRelation.toRelation(sql);
    QueryExecutionPlan queryExecutionPlan = QueryExecutionPlanFactory.create(newSchema, null, selectQuery);
    ExecutableNodeBase copy = queryExecutionPlan.root.getExecutableNodeBaseDependent(0).getExecutableNodeBaseDependent(0).deepcopy();
    QueryExecutionPlan plan = QueryExecutionPlanSimplifier.simplify(queryExecutionPlan);

    assertEquals(0, plan.root.getDependentNodeCount());

    assertEquals(
        ((QueryNodeBase) copy).selectQuery,
        ((SubqueryColumn)
            ((ColumnOp)
                ((SelectQuery) 
                    ((QueryNodeBase) plan.root)
                    .selectQuery.getFromList().get(0)).getFilter().get()).getOperand(1)).getSubquery());
    // queryExecutionPlan.root.execute(conn);
  }
}
