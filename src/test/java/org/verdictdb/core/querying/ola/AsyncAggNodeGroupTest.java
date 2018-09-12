package org.verdictdb.core.querying.ola;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.junit.Test;
import org.verdictdb.core.querying.ExecutableNodeBase;
import org.verdictdb.core.querying.QueryExecutionPlan;
import org.verdictdb.core.querying.QueryExecutionPlanFactory;
import org.verdictdb.core.scrambling.ScrambleMeta;
import org.verdictdb.core.scrambling.ScrambleMetaSet;
import org.verdictdb.core.sqlobject.SelectQuery;
import org.verdictdb.exception.VerdictDBException;
import org.verdictdb.sqlreader.NonValidatingSQLParser;

public class AsyncAggNodeGroupTest {

  @Test
  public void test() throws VerdictDBException {
    // set up scramble meta
    ScrambleMetaSet meta = new ScrambleMetaSet();
    HashMap<Integer, List<Double>> distribution = new HashMap<>();
    distribution.put(0, Arrays.asList(0.2, 0.5, 1.0));
    ScrambleMeta tablemeta = 
        new ScrambleMeta(
            "newSchema", "scrambledTable",
            "originalSchema", "originalTable",
            "blockColumn", 3,
            "tierColumn", 1,
            distribution);
//    tablemeta.setCumulativeMassDistributionPerTier(distribution);
    meta.addScrambleMeta(tablemeta);
    
    // compose a query
    String sql = "select sum(t.price) as s from newSchema.scrambledTable as t";
    NonValidatingSQLParser parser = new NonValidatingSQLParser();
    SelectQuery query = (SelectQuery) parser.toRelation(sql);
    
    QueryExecutionPlan plan = 
        QueryExecutionPlanFactory.create("verdictdb_temp1", meta, query);
//    plan.getRoot().print();
    
    QueryExecutionPlan asyncPlan = AsyncQueryExecutionPlan.create(plan);
//    asyncPlan.getRoot().print();
    
    ExecutableNodeBase root = asyncPlan.getRoot();
    
    assertEquals(5, asyncPlan.getNodeGroupIDs().size());
    assertEquals(
        root.getExecutableNodeBaseDependent(0)
        .getExecutableNodeBaseDependent(0).getGroupId(),
        root.getExecutableNodeBaseDependent(0)
        .getExecutableNodeBaseDependent(1).getExecutableNodeBaseDependent(0).getGroupId());
  }

}
