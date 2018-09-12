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

package org.verdictdb.core.querying.ola;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.verdictdb.core.execplan.ExecutableNode;
import org.verdictdb.core.querying.*;
import org.verdictdb.core.scrambling.ScrambleMeta;
import org.verdictdb.core.scrambling.ScrambleMetaSet;
import org.verdictdb.core.sqlobject.AbstractRelation;
import org.verdictdb.core.sqlobject.AliasedColumn;
import org.verdictdb.core.sqlobject.AsteriskColumn;
import org.verdictdb.core.sqlobject.BaseColumn;
import org.verdictdb.core.sqlobject.BaseTable;
import org.verdictdb.core.sqlobject.ColumnOp;
import org.verdictdb.core.sqlobject.ConstantColumn;
import org.verdictdb.core.sqlobject.JoinTable;
import org.verdictdb.core.sqlobject.SelectItem;
import org.verdictdb.core.sqlobject.SelectQuery;
import org.verdictdb.core.sqlobject.UnnamedColumn;
import org.verdictdb.exception.VerdictDBException;
import org.verdictdb.exception.VerdictDBTypeException;
import org.verdictdb.exception.VerdictDBValueException;

/**
 * An online aggregation (or approximate aggregation) version of the given QueryExecutionPlan.
 *
 * @author Yongjoo Park
 */
public class AsyncQueryExecutionPlan extends QueryExecutionPlan {

  private static final long serialVersionUID = -1670795390245860583L;

  private int aggColumnIdentiferNum = 0;

  private static final String TIER_COLUMN_ALIAS_KEYWORD = "tier";

  private AsyncQueryExecutionPlan(String scratchpadSchemaName, ScrambleMetaSet scrambleMeta) {
    super(scratchpadSchemaName, scrambleMeta);
  }

  private AsyncQueryExecutionPlan(IdCreator idCreator, ScrambleMetaSet scrambleMeta) {
    super(idCreator, scrambleMeta);
  }

  public static AsyncQueryExecutionPlan create(QueryExecutionPlan plan) throws VerdictDBException {
    if (plan instanceof AsyncQueryExecutionPlan) {
      System.err.println("It is already an asyncronous plan.");
      throw new VerdictDBTypeException(plan);
    }

    AsyncQueryExecutionPlan asyncPlan =
        new AsyncQueryExecutionPlan(plan.getIdCreator(), plan.getScrambleMeta());
    ExecutableNodeBase newRoot = asyncPlan.makeAsyncronousAggIfAvailable(plan.getRootNode());
    asyncPlan.setRootNode(newRoot);
    return asyncPlan;
  }

  /**
   * Returns an asynchronous version of the given plan.
   *
   * @param root The root execution node of ALL nodes (i.e., not just the top agg node)
   * @return
   * @throws VerdictDBException
   */
  ExecutableNodeBase makeAsyncronousAggIfAvailable(ExecutableNodeBase root)
      throws VerdictDBException {
    List<AggExecutionNodeBlock> aggBlocks = identifyTopAggBlocks(scrambleMeta, root);

    // converted nodes should be used in place of the original nodes.
    for (int i = 0; i < aggBlocks.size(); i++) {
      // this node block contains the links to those nodes belonging to this block.
      AggExecutionNodeBlock nodeBlock = aggBlocks.get(i);

      ExecutableNodeBase oldNode = nodeBlock.getBlockRootNode();
      ExecutableNodeBase newNode = convertToProgressiveAgg(scrambleMeta, nodeBlock);

      List<ExecutableNodeBase> parents = oldNode.getExecutableNodeBaseParents();
      for (ExecutableNodeBase parent : parents) {
        Integer channel = parent.getChannelForSource(oldNode);
        if (channel == null) {
          // do nothing
        } else {
          parent.cancelSubscriptionTo(oldNode);
          parent.subscribeTo(newNode, channel);
        }
      }
    }

    return root;
  }

  /**
   * Converts the root node and its descendants into the configuration that enables progressive
   * aggregation.
   *
   * <p>Basically aggregate subqueries are blocking operations while others operations are divided
   * into smaller- scale operations (which involve different portions of data).
   *
   * @param scrambleMeta The metadata about the scrambled tables.
   * @param aggNodeBlock A set of the links to the nodes that will be processed in the asynchronous
   *                     manner.
   * @return Returns The root of the multiple aggregation nodes (each of which involves different
   * combinations of partitions)
   * @throws VerdictDBValueException
   */
  public ExecutableNodeBase convertToProgressiveAgg(
      ScrambleMetaSet scrambleMeta, AggExecutionNodeBlock aggNodeBlock)
      throws VerdictDBValueException {

    List<ExecutableNodeBase> blockNodes = aggNodeBlock.getNodesInBlock();

    List<ExecutableNodeBase> individualAggNodes = new ArrayList<>();
    List<ExecutableNodeBase> combiners = new ArrayList<>();
    List<AggExecutionNodeBlock> aggblocks = new ArrayList<>();
    //    ScrambleMeta scrambleMeta = idCreator.getScrambleMeta();

    // First, plan how to perform block aggregation
    // filtering predicates that must inserted into different scrambled tables are identified.
    List<Pair<ExecutableNodeBase, Triple<String, String, String>>> scrambledNodes =
        identifyScrambledNodes(scrambleMeta, blockNodes);
    List<Pair<String, String>> scrambles = new ArrayList<>();
    for (Pair<ExecutableNodeBase, Triple<String, String, String>> a : scrambledNodes) {
      String schemaName = a.getRight().getLeft();
      String tableName = a.getRight().getMiddle();
      scrambles.add(Pair.of(schemaName, tableName));
    }
    OlaAggregationPlan aggPlan = new OlaAggregationPlan(scrambleMeta, scrambles);
    List<Pair<ExecutableNodeBase, ExecutableNodeBase>> oldSubscriptionInformation =
        new ArrayList<>();

    // Second, according to the plan, create individual nodes that perform aggregations.
    for (int i = 0; i < aggPlan.totalBlockAggCount(); i++) {

      // copy and remove the dependency to its parents
      oldSubscriptionInformation.clear();
      AggExecutionNodeBlock copy =
          aggNodeBlock.deepcopyExcludingDependentAggregates(oldSubscriptionInformation);
      AggExecutionNode aggroot = (AggExecutionNode) copy.getBlockRootNode();
      for (ExecutableNodeBase parent : aggroot.getExecutableNodeBaseParents()) {
        parent.cancelSubscriptionTo(aggroot); // not sure if this is required, but do anyway
      }
      aggroot.cancelSubscriptionsFromAllSubscribers(); // subscription will be reconstructed later.

      // Add extra predicates to restrain each aggregation to particular parts of base tables.
      List<Pair<ExecutableNodeBase, Triple<String, String, String>>> scrambledNodeAndTableName =
          identifyScrambledNodes(scrambleMeta, copy.getNodesInBlock());

      // Assign hyper table cube to the block
      aggroot.getAggMeta().setCubes(Arrays.asList(aggPlan.cubes.get(i)));

      // rewrite the select list of the individual aggregate nodes to add tier columns
      resetTierColumnAliasGeneration();
      addTierColumnsRecursively(copy, aggroot, new HashSet<ExecutableNode>());

      // Insert predicates into individual aggregation nodes
      for (Pair<ExecutableNodeBase, Triple<String, String, String>> a : scrambledNodeAndTableName) {
        ExecutableNodeBase scrambledNode = a.getLeft();
        String schemaName = a.getRight().getLeft();
        String tableName = a.getRight().getMiddle();
        String aliasName = a.getRight().getRight();
        Pair<Integer, Integer> span = aggPlan.getAggBlockSpanForTable(schemaName, tableName, i);
        String aggblockColumn = scrambleMeta.getAggregationBlockColumn(schemaName, tableName);
        SelectQuery q = ((QueryNodeBase) scrambledNode).getSelectQuery();
        //        String aliasName = findAliasFor(schemaName, tableName, q.getFromList());
        if (aliasName == null) {
          throw new VerdictDBValueException(
              String.format(
                  "The alias name for the table (%s, %s) is not found.", schemaName, tableName));
        }

        int left = span.getLeft();
        int right = span.getRight();
        if (left == right) {
          q.addFilterByAnd(
              ColumnOp.equal(
                  new BaseColumn(aliasName, aggblockColumn), ConstantColumn.valueOf(left)));
        } else {
          q.addFilterByAnd(
              ColumnOp.greaterequal(
                  new BaseColumn(aliasName, aggblockColumn), ConstantColumn.valueOf(left)));
          q.addFilterByAnd(
              ColumnOp.lessequal(
                  new BaseColumn(aliasName, aggblockColumn), ConstantColumn.valueOf(right)));
        }
      }

      individualAggNodes.add(aggroot);
      aggblocks.add(copy);
    }

    // Third, stack combiners
    // clear existing broadcasting queues of individual agg nodes
    for (ExecutableNodeBase n : individualAggNodes) {
      n.cancelSubscriptionsFromAllSubscribers();
    }
    for (int i = 1; i < aggPlan.totalBlockAggCount(); i++) {
      AggCombinerExecutionNode combiner;
      if (i == 1) {
        combiner =
            AggCombinerExecutionNode.create(
                idCreator, individualAggNodes.get(0), individualAggNodes.get(1));
      } else {
        combiner =
            AggCombinerExecutionNode.create(
                idCreator, combiners.get(i - 2), individualAggNodes.get(i));
      }
      combiners.add(combiner);
    }

    // Fourth, re-link the subscription relationship for the new AsyncAggNode
    ExecutableNodeBase newRoot =
        AsyncAggExecutionNode.create(idCreator, aggblocks, combiners, scrambleMeta, aggNodeBlock);

    // Finally remove the old subscription information: old copied node -> still used old node
    for (Pair<ExecutableNodeBase, ExecutableNodeBase> parentToSource : oldSubscriptionInformation) {
      ExecutableNodeBase subscriber = parentToSource.getLeft();
      ExecutableNodeBase source = parentToSource.getRight();
      subscriber.cancelSubscriptionTo(source);
    }

    return newRoot;
  }

  /**
   * @param scrambleMeta Information about what tables have been scrambled.
   * @param blockNodes
   * @return ExecutableNodeBase is the reference to the scrambled base table. The triple is (schema,
   * table, alias) of scrambled tables.
   */
  private static List<Pair<ExecutableNodeBase, Triple<String, String, String>>>
  identifyScrambledNodes(ScrambleMetaSet scrambleMeta, List<ExecutableNodeBase> blockNodes) {

    List<Pair<ExecutableNodeBase, Triple<String, String, String>>> identified = new ArrayList<>();

    for (ExecutableNodeBase node : blockNodes) {
      for (AbstractRelation rel : ((QueryNodeBase) node).getSelectQuery().getFromList()) {
        if (rel instanceof BaseTable) {
          BaseTable base = (BaseTable) rel;
          if (scrambleMeta.isScrambled(base.getSchemaName(), base.getTableName())) {
            identified.add(
                Pair.of(
                    node,
                    Triple.of(
                        base.getSchemaName(), base.getTableName(), base.getAliasName().get())));
          }
        } else if (rel instanceof JoinTable) {
          for (AbstractRelation r : ((JoinTable) rel).getJoinList()) {
            if (r instanceof BaseTable) {
              BaseTable base = (BaseTable) r;
              if (scrambleMeta.isScrambled(base.getSchemaName(), base.getTableName())) {
                identified.add(
                    Pair.of(
                        node,
                        Triple.of(
                            base.getSchemaName(), base.getTableName(), base.getAliasName().get())));
              }
            }
          }
        }
      }
    }

    return identified;
  }

  private List<ColumnOp> getAggregateColumn(UnnamedColumn sel) {
    List<SelectItem> itemToCheck = new ArrayList<>();
    itemToCheck.add(sel);
    List<ColumnOp> columnOps = new ArrayList<>();
    while (!itemToCheck.isEmpty()) {
      SelectItem s = itemToCheck.get(0);
      itemToCheck.remove(0);
      if (s instanceof ColumnOp) {
        if (((ColumnOp) s).getOpType().equals("count")
            || ((ColumnOp) s).getOpType().equals("sum")
            || ((ColumnOp) s).getOpType().equals("avg")
            || ((ColumnOp) s).getOpType().equals("max")
            || ((ColumnOp) s).getOpType().equals("min")) {
          columnOps.add((ColumnOp) s);
        } else itemToCheck.addAll(((ColumnOp) s).getOperands());
      }
    }
    return columnOps;
  }

  /**
   * identify the nodes that are (1) aggregates with scrambled tables, (2) no descendants of any
   * other top aggregates, (3) the aggregated columns (inner-most base columns if they are inside
   * some functions) must include only the columns from the scrambled tables.
   */
  private List<AggExecutionNodeBlock> identifyTopAggBlocks(
      ScrambleMetaSet scrambleMeta, ExecutableNodeBase root) {
    List<AggExecutionNodeBlock> aggblocks = new ArrayList<>();
    //    ScrambleMeta scrambleMeta = root.getPlan().getScrambleMeta();

    if (root instanceof AggExecutionNode) {
      // check if it contains at least one scrambled table.
      if (doesContainScramble(root, scrambleMeta)) {
        AggExecutionNodeBlock block = new AggExecutionNodeBlock(root);
        aggblocks.add(block);
        return aggblocks;
      }
    }

    for (ExecutableNodeBase dep : root.getExecutableNodeBaseDependents()) {
      List<AggExecutionNodeBlock> depAggBlocks = identifyTopAggBlocks(scrambleMeta, dep);
      aggblocks.addAll(depAggBlocks);
    }

    return aggblocks;
  }


  private boolean doesContainScramble(ExecutableNodeBase node, ScrambleMetaSet scrambleMeta) {
    SelectQuery query = ((QueryNodeBase) node).getSelectQuery();

    // check within the query
    for (AbstractRelation rel : query.getFromList()) {
      if (rel instanceof BaseTable) {
        BaseTable base = (BaseTable) rel;
        String schemaName = base.getSchemaName();
        String tableName = base.getTableName();
        if (scrambleMeta.isScrambled(schemaName, tableName)) {
          return true;
        }
      } else if (rel instanceof JoinTable) {
        for (AbstractRelation r : ((JoinTable) rel).getJoinList()) {
          if (r instanceof BaseTable) {
            BaseTable base = (BaseTable) r;
            String schemaName = base.getSchemaName();
            String tableName = base.getTableName();
            if (scrambleMeta.isScrambled(schemaName, tableName)) {
              return true;
            }
          }
        }
      }
      // SelectQuery is not supposed to be passed.
    }

    for (ExecutableNodeBase dep : node.getExecutableNodeBaseDependents()) {
      if (dep instanceof AggExecutionNode) {
        continue;
      }
      if (doesContainScramble(dep, scrambleMeta)) {
        return true;
      }
    }
    return false;
  }

  private List<BaseTable> identifyScrambledTables(
      ExecutableNodeBase node, ScrambleMetaSet scrambleMeta) {

    SelectQuery query = ((QueryNodeBase) node).getSelectQuery();
    List<BaseTable> multiTierScrambleTables = new ArrayList<>();

    // check within the query
    for (AbstractRelation rel : query.getFromList()) {
      if (rel instanceof BaseTable) {
        BaseTable base = (BaseTable) rel;
        String schemaName = base.getSchemaName();
        String tableName = base.getTableName();
        //        if (scrambleMeta.isScrambled(schemaName, tableName)
        //            && scrambleMeta.getSingleMeta(schemaName, tableName).getNumberOfTiers() > 1) {
        if (scrambleMeta.isScrambled(schemaName, tableName)) {
          multiTierScrambleTables.add(base);
        }
      } else if (rel instanceof JoinTable) {
        for (AbstractRelation r : ((JoinTable) rel).getJoinList()) {
          if (r instanceof BaseTable) {
            BaseTable base = (BaseTable) r;
            String schemaName = base.getSchemaName();
            String tableName = base.getTableName();
            //            if (scrambleMeta.isScrambled(schemaName, tableName)
            //                && scrambleMeta.getSingleMeta(schemaName,
            // tableName).getNumberOfTiers() > 1) {
            if (scrambleMeta.isScrambled(schemaName, tableName)) {
              multiTierScrambleTables.add(base);
            }
          }
        }
      }
      // SelectQuery is not supposed to be passed.
    }

    return multiTierScrambleTables;
  }

  /**
   * Rewrite the select list of the nodes in a block; the nodes that belong ot the block are
   * identified by the second parameter `nodeList`.
   *
   * @param root
   * @param nodeList
   * @return
   */
  private ExecutableNodeBase rewriteSelectListOfRootAndListedDependents(
      ExecutableNodeBase root, List<ExecutableNodeBase> nodeList) {
    List<ExecutableNodeBase> visitList = new ArrayList<>();
    ExecutableNodeBase rewritten =
        rewriteSelectListOfRootAndListedDependentsInner(root, nodeList, visitList);
    return rewritten;
  }

  private ExecutableNodeBase rewriteSelectListOfRootAndListedDependentsInner(
      ExecutableNodeBase root,
      List<ExecutableNodeBase> nodeList,
      List<ExecutableNodeBase> visitList) {

    // base condition: return immediately if the root does not belong to nodeList.

    // base condition: return immediately if this node has been visited before.

    // call rewriteSelectListOfRootAndListedDependents for all dependents first

    // then does the job for the root node.

    return null;
  }

  private void rewriteProjectionNodeForMultiTier(
      ProjectionNode node, List<BaseTable> scrambledTables, ScrambleMetaSet scrambleMeta) {

//    List<SelectItem> selectItemList = node.getSelectQuery().getSelectList();

    for (BaseTable t : scrambledTables) {
      // Add tier column to the select list
      String tierColumnName = scrambleMeta.getTierColumn(t.getSchemaName(), t.getTableName());
      SelectItem tierColumn;
      String tierColumnAlias = generateTierColumnAliasName();
      //        VERDICTDB_TIER_COLUMN_NAME + verdictdbTierIndentiferNum++;
      if (t.getAliasName().isPresent()) {
        tierColumn =
            new AliasedColumn(
                new BaseColumn(t.getAliasName().get(), tierColumnName), tierColumnAlias);
      } else {
        tierColumn =
            new AliasedColumn(new BaseColumn(t.getTableName(), tierColumnName), tierColumnAlias);
      }
      //      selectItemList.add(tierColumn);
      node.getSelectQuery().addSelectItem(tierColumn);

      // ProjectionNode allow to have group by only if group by refers to all select items
      if (!node.getSelectQuery().getGroupby().isEmpty()) {
        node.getSelectQuery().addGroupby(((AliasedColumn) tierColumn).getColumn());
      }

      // Record the tier column alias with its corresponding scramble table
      ScrambleMeta meta = scrambleMeta.getSingleMeta(t.getSchemaName(), t.getTableName());
      node.getAggMeta().getTierColumnForScramble().put(meta, tierColumnAlias);
    }

//    node.getSelectQuery().addSelectItem();

//    List<SelectItem> selectItemList = node.getSelectQuery().getSelectList();
//    if (selectItemList.get(0) instanceof AsteriskColumn) {
//      for (BaseTable t : MultiTiertables) {
//        String tierColumnAlias = generateTierColumnAliasName();
////            VERDICTDB_TIER_COLUMN_NAME + verdictdbTierIndentiferNum++;
//
//        // Record the tier column alias with its corresponding scramble table
//        ScrambleMeta meta = scrambleMeta.getSingleMeta(t.getSchemaName(), t.getTableName());
//        node.getAggMeta().getTierColumnForScramble().put(meta, tierColumnAlias);
//      }
//    } else {
//      for (BaseTable t : MultiTiertables) {
//        // Add tier column to the select list
//        String tierColumnName = scrambleMeta.getTierColumn(t.getSchemaName(), t.getTableName());
//        SelectItem tierColumn;
//        String tierColumnAlias = generateTierColumnAliasName();
////        VERDICTDB_TIER_COLUMN_NAME + verdictdbTierIndentiferNum++;
//        if (t.getAliasName().isPresent()) {
//          tierColumn =
//              new AliasedColumn(
//                  new BaseColumn(t.getAliasName().get(), tierColumnName), tierColumnAlias);
//        } else {
//          tierColumn =
//              new AliasedColumn(new BaseColumn(t.getTableName(), tierColumnName), tierColumnAlias);
//        }
//        selectItemList.add(tierColumn);
//
//        // Record the tier column alias with its corresponding scramble table
//        ScrambleMeta meta = scrambleMeta.getSingleMeta(t.getSchemaName(), t.getTableName());
//        node.getAggMeta().getTierColumnForScramble().put(meta, tierColumnAlias);
//      }
//    }
//    verdictdbTierIndentiferNum = 0;
  }

  void addTierColumnsRecursively(
      AggExecutionNodeBlock block, ExecutableNodeBase node, Set<ExecutableNode> visitList) {

    // rewrite all the sources
    for (ExecutableNodeBase source : node.getSources()) {
      if (!visitList.contains(source) && block.getNodesInBlock().contains(source)) {
        addTierColumnsRecursively(block, source, visitList);
      }
    }

    // rewrite the current node
    visitList.add(node);
    if (node instanceof AggExecutionNode) {
      List<SelectItem> newSelectlist =
          createUnfoldSelectlistWithBasicAgg(
              ((AggExecutionNode) node).getSelectQuery(), node.getAggMeta());

      List<ProjectionNode> projectionNodeList = new ArrayList<>();
      for (ExecutableNodeBase source : node.getSources()) {
        if (source instanceof ProjectionNode && block.getNodesInBlock().contains(source)) {
          projectionNodeList.add((ProjectionNode) source);
        }
      }
      Map<ScrambleMeta, String> scrambleMetaAndTierColumnAlias =
          addTierColumnToSelectListAndGroupBy(
              ((AggExecutionNode) node).getSelectQuery(),
              newSelectlist,
              scrambleMeta,
              projectionNodeList);
      ((AggExecutionNode) node).getSelectQuery().clearSelectList();
      ((AggExecutionNode) node).getSelectQuery().getSelectList().addAll(newSelectlist);
      aggColumnIdentiferNum = 0;

      node.getAggMeta().setTierColumnForScramble(scrambleMetaAndTierColumnAlias);

    } else if (node instanceof ProjectionNode) {
      rewriteProjectionNodeToAddTierColumn(block, (ProjectionNode) node);
    }
  }

  /*-
   * For example, convert
   *
   * 1. avg(price) ---------------------> sum(price) as 'agg0', count(price) as 'agg1'
   * 2. sum(price) / count(*) ----------> sum(price) as 'agg0', count(*) as 'agg1'
   *
   * @param query
   * @param meta
   * @return
   */
  private List<SelectItem> createUnfoldSelectlistWithBasicAgg(SelectQuery query, AggMeta meta) {

    List<SelectItem> selectList = query.getSelectList();
    List<String> aggColumnAlias = new ArrayList<>();
    HashMap<String, String> maxminAlias = new HashMap<>();
    List<SelectItem> newSelectlist = new ArrayList<>();
    meta.setOriginalSelectList(selectList);

    for (SelectItem selectItem : selectList) {
      if (selectItem instanceof AliasedColumn) {
        AliasedColumn ac = (AliasedColumn) selectItem;
        String newAlias = "";
        String prefix = "";
        // Set alias of the new select items accordingly
        if (ac.getAliasName().startsWith(AsyncAggExecutionNode.getHavingConditionAlias())
            || ac.getAliasName().startsWith(AsyncAggExecutionNode.getGroupByAlias())
            || ac.getAliasName().startsWith(AsyncAggExecutionNode.getOrderByAlias())) {
          prefix = ac.getAliasName();
          newAlias = ac.getAliasName();
        } else {
          prefix = "agg";
          newAlias = prefix + aggColumnIdentiferNum;
        }
        List<ColumnOp> columnOps = getAggregateColumn(((AliasedColumn) selectItem).getColumn());
        // If it contains agg columns
        if (!columnOps.isEmpty()) {
          meta.getAggColumn().put(selectItem, columnOps);
          int cnt = 0;
          for (ColumnOp col : columnOps) {
            if (ac.getAliasName().startsWith(AsyncAggExecutionNode.getHavingConditionAlias())
                || ac.getAliasName().startsWith(AsyncAggExecutionNode.getOrderByAlias())) {
              newAlias = prefix + "_" + cnt;
              ++cnt;
            }
            if (col.getOpType().equals("avg")) {
              if (!meta.getAggColumnAggAliasPair()
                  .containsKey(new ImmutablePair<>("sum", col.getOperand(0)))) {
                ColumnOp col1 = new ColumnOp("sum", col.getOperand(0));
                newSelectlist.add(new AliasedColumn(col1, newAlias));
                meta.getAggColumnAggAliasPair()
                    .put(new ImmutablePair<>("sum", col1.getOperand(0)), newAlias);
                aggColumnAlias.add(newAlias);
                ++aggColumnIdentiferNum;
              } else if (!newAlias.startsWith("agg")) {
                ColumnOp col1 = new ColumnOp("sum", col.getOperand(0));
                newSelectlist.add(new AliasedColumn(col1, newAlias));
                aggColumnAlias.add(newAlias);
              }
              if (!meta.getAggColumnAggAliasPair()
                  .containsKey(
                      new ImmutablePair<>("count", (UnnamedColumn) new AsteriskColumn()))) {
                if (prefix.equals("agg")) {
                  newAlias = prefix + aggColumnIdentiferNum;
                } else {
                  newAlias += "_cnt";
                }
                ++aggColumnIdentiferNum;
                ColumnOp col2 = new ColumnOp("count", new AsteriskColumn());
                newSelectlist.add(new AliasedColumn(col2, newAlias));
                meta.getAggColumnAggAliasPair()
                    .put(
                        new ImmutablePair<>("count", (UnnamedColumn) new AsteriskColumn()),
                        newAlias);
                aggColumnAlias.add(newAlias);
              }
            } else if (col.getOpType().equals("count") || col.getOpType().equals("sum")) {
              if (col.getOpType().equals("count")) {
                if (!meta.getAggColumnAggAliasPair()
                    .containsKey(
                        new ImmutablePair<>("count", (UnnamedColumn) (new AsteriskColumn())))
                    && (col.getOperands().isEmpty()
                      || col.getOperand() instanceof AsteriskColumn
                      || col.getOperand() instanceof ConstantColumn
                      || col.getOperand() instanceof BaseColumn)) {
                  ColumnOp col1 = new ColumnOp(col.getOpType());
                  newSelectlist.add(new AliasedColumn(col1, newAlias));
                  meta.getAggColumnAggAliasPair()
                      .put(
                          new ImmutablePair<>(
                              col.getOpType(), (UnnamedColumn) new AsteriskColumn()),
                          newAlias);
                  aggColumnAlias.add(newAlias);
                  ++aggColumnIdentiferNum;
                } else if (col.getOperand(0) instanceof ColumnOp && !meta.getAggColumnAggAliasPair()
                    .containsKey(new ImmutablePair<>(col.getOpType(), col.getOperand(0)))) {
                  ColumnOp col1 = new ColumnOp(col.getOpType(), col.getOperand(0));
                  newSelectlist.add(new AliasedColumn(col1, newAlias));
                  meta.getAggColumnAggAliasPair()
                      .put(new ImmutablePair<>(col.getOpType(), col1.getOperand(0)), newAlias);
                  aggColumnAlias.add(newAlias);
                  ++aggColumnIdentiferNum;
                } else if (!newAlias.startsWith("agg")) {
                  ColumnOp col1 = new ColumnOp("count", col.getOperand(0));
                  newSelectlist.add(new AliasedColumn(col1, newAlias));
                  aggColumnAlias.add(newAlias);
                }
              } else if (col.getOpType().equals("sum")) {
                if (!meta.getAggColumnAggAliasPair()
                    .containsKey(new ImmutablePair<>(col.getOpType(), col.getOperand(0)))) {
                  ColumnOp col1 = new ColumnOp(col.getOpType(), col.getOperand(0));
                  newSelectlist.add(new AliasedColumn(col1, newAlias));
                  meta.getAggColumnAggAliasPair()
                      .put(new ImmutablePair<>(col.getOpType(), col1.getOperand(0)), newAlias);
                  aggColumnAlias.add(newAlias);
                  ++aggColumnIdentiferNum;
                } else if (!newAlias.startsWith("agg")) {
                  ColumnOp col1 = new ColumnOp("sum", col.getOperand(0));
                  newSelectlist.add(new AliasedColumn(col1, newAlias));
                  aggColumnAlias.add(newAlias);
                }
              }
            } else if (col.getOpType().equals("max") || col.getOpType().equals("min")) {
              ColumnOp col1 = new ColumnOp(col.getOpType(), col.getOperand(0));
              newSelectlist.add(new AliasedColumn(col1, newAlias));
              meta.getAggColumnAggAliasPairOfMaxMin()
                  .put(new ImmutablePair<>(col.getOpType(), col1.getOperand(0)), newAlias);
              maxminAlias.put(newAlias, col.getOpType());
              ++aggColumnIdentiferNum;
            }
            if (prefix.equals("agg")) {
              newAlias = prefix + aggColumnIdentiferNum;
            }
          }
        } else {
          newSelectlist.add(selectItem.deepcopy());
        }
      } else {
        newSelectlist.add(selectItem.deepcopy());
      }
    }
    meta.setAggAlias(aggColumnAlias);
    meta.setMaxminAggAlias(maxminAlias);
    return newSelectlist;
  }

  /**
   * Adds tier expressions to the end of the select list; and to the group-by list.
   *
   * @param query
   * @param newSelectList
   * @param scrambleMetaSet
   */
  private Map<ScrambleMeta, String> addTierColumnToSelectListAndGroupBy(
      SelectQuery query,
      List<SelectItem> newSelectList,
      ScrambleMetaSet scrambleMetaSet,
      List<ProjectionNode> projectionNodeSources) {

    Map<ScrambleMeta, String> scrambleMetaAnditsAlias = new HashMap<>();

    for (AbstractRelation table : query.getFromList()) {
      if (table instanceof BaseTable) {
        String schemaName = ((BaseTable) table).getSchemaName();
        String tableName = ((BaseTable) table).getTableName();

        if (scrambleMetaSet.isScrambled(schemaName, tableName)) {
          ScrambleMeta singleMeta = scrambleMetaSet.getSingleMeta(schemaName, tableName);
          String tierColumnName = scrambleMetaSet.getTierColumn(schemaName, tableName);
          String newTierColumnAlias = generateTierColumnAliasName();
          //          VERDICTDB_TIER_COLUMN_NAME + verdictdbTierIndentiferNum++;
          BaseColumn tierColumn =
              new BaseColumn(schemaName, tableName, table.getAliasName().get(), tierColumnName);
          newSelectList.add(new AliasedColumn(tierColumn, newTierColumnAlias));
          query.addGroupby(tierColumn);

          // Add to the tier column Map
          scrambleMetaAnditsAlias.put(singleMeta, newTierColumnAlias);
        }
      } else if (table instanceof JoinTable) {
        for (AbstractRelation jointable : ((JoinTable) table).getJoinList()) {
          if (jointable instanceof BaseTable) {
            String schemaName = ((BaseTable) jointable).getSchemaName();
            String tableName = ((BaseTable) jointable).getTableName();

            if (scrambleMetaSet.isScrambled(schemaName, tableName)) {
              ScrambleMeta singleMeta = scrambleMetaSet.getSingleMeta(schemaName, tableName);
              String tierColumnName = scrambleMetaSet.getTierColumn(schemaName, tableName);
              String newTierColumnAlias = generateTierColumnAliasName();
              //              VERDICTDB_TIER_COLUMN_NAME + verdictdbTierIndentiferNum++;
              BaseColumn tierColumn =
                  new BaseColumn(
                      schemaName, tableName, jointable.getAliasName().get(), tierColumnName);
              newSelectList.add(new AliasedColumn(tierColumn, newTierColumnAlias));
              query.addGroupby(tierColumn);

              // Add to the tier column Map
              scrambleMetaAnditsAlias.put(singleMeta, newTierColumnAlias);
            }
          }
        }
      }
    }

    // Add possible tier column if its sources are projectionNode
    for (ProjectionNode source : projectionNodeSources) {
      for (Map.Entry<ScrambleMeta, String> entry :
          source.getAggMeta().getTierColumnForScramble().entrySet()) {

        ScrambleMeta singleMeta = entry.getKey();
        String oldtierAlias = entry.getValue();

        // Add tier column to select list
        SelectItem selectItem;
        String newTierColumnAlias = generateTierColumnAliasName();
        UnnamedColumn column;
        //        VERDICTDB_TIER_COLUMN_NAME + verdictdbTierIndentiferNum++;
        if (source.getSelectQuery().getAliasName().isPresent()) {
          String sourceAlias = source.getSelectQuery().getAliasName().get();
          column = new BaseColumn(sourceAlias, oldtierAlias);
          selectItem = new AliasedColumn(column, newTierColumnAlias);
        } else {
          column = new BaseColumn(oldtierAlias);
          selectItem = new AliasedColumn(column, newTierColumnAlias);
        }
        newSelectList.add(selectItem);

        query.addGroupby(column);

        // Add to the tier column Map
        scrambleMetaAnditsAlias.put(singleMeta, newTierColumnAlias);
      }
    }
//    verdictdbTierIndentiferNum = 0;

    return scrambleMetaAnditsAlias;
  }

  private void rewriteProjectionNodeToAddTierColumn(
      AggExecutionNodeBlock block, ProjectionNode node) {
    // If it is a leaf node, check whether it contains scramble table
    if (node.getSources().size() == 0) {
      List<BaseTable> multiTierScrambleTables = identifyScrambledTables(node, scrambleMeta);
      // rewrite itself
      if (!multiTierScrambleTables.isEmpty()) {
        rewriteProjectionNodeForMultiTier(node, multiTierScrambleTables, scrambleMeta);
      }
      return;
    }

    // Otherwise, we first need to call the function recursively to the sources
    List<ProjectionNode> projectionNodesSources = new ArrayList<>();
    for (ExecutableNode source : node.getSources()) {
      if (source instanceof ProjectionNode && block.getNodesInBlock().contains(source)) {
        projectionNodesSources.add((ProjectionNode) source);
      }
    }


    // Add tier column if its placeholder table has scramble table
    List<SelectItem> selectItemList = node.getSelectQuery().getSelectList();
    for (ProjectionNode source : projectionNodesSources) {
      for (Map.Entry<ScrambleMeta, String> entry :
          source.getAggMeta().getTierColumnForScramble().entrySet()) {

        String oldtierAlias = entry.getValue();
        String tierColumnAlias = generateTierColumnAliasName();
//        VERDICTDB_TIER_COLUMN_NAME + verdictdbTierIndentiferNum++;

        // Add tier column to select list
        SelectItem selectItem;
        if (source.getSelectQuery().getAliasName().isPresent()) {
          String sourceAlias = source.getSelectQuery().getAliasName().get();
          selectItem =
              new AliasedColumn(new BaseColumn(sourceAlias, oldtierAlias), tierColumnAlias);
        } else {
          selectItem = new AliasedColumn(new BaseColumn(oldtierAlias), tierColumnAlias);
        }
        selectItemList.add(selectItem);

        // Construct tier column Map
        node.getAggMeta().getTierColumnForScramble().put(entry.getKey(), tierColumnAlias);
      }
    }
    List<BaseTable> multiTierScrambleTables = identifyScrambledTables(node, scrambleMeta);

    // Add tier column if itself contain scramble table
    if (!multiTierScrambleTables.isEmpty()) {
      rewriteProjectionNodeForMultiTier(node, multiTierScrambleTables, scrambleMeta);
    }
    //    verdictdbTierIndentiferNum = 0;
  }

  private String generateTierColumnAliasName() {
    return generateAliasName(TIER_COLUMN_ALIAS_KEYWORD);
  }

  private void resetTierColumnAliasGeneration() {
    resetAliasNameGeneration(TIER_COLUMN_ALIAS_KEYWORD);
  }
}
