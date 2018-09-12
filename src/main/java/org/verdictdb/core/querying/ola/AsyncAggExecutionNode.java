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

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.verdictdb.connection.DbmsQueryResult;
import org.verdictdb.core.execplan.ExecutionInfoToken;
import org.verdictdb.core.querying.AggExecutionNode;
import org.verdictdb.core.querying.ExecutableNodeBase;
import org.verdictdb.core.querying.IdCreator;
import org.verdictdb.core.querying.ProjectionNode;
import org.verdictdb.core.querying.QueryNodeBase;
import org.verdictdb.core.querying.SubscriptionTicket;
import org.verdictdb.core.scrambling.ScrambleMeta;
import org.verdictdb.core.scrambling.ScrambleMetaSet;
import org.verdictdb.core.sqlobject.AliasedColumn;
import org.verdictdb.core.sqlobject.AsteriskColumn;
import org.verdictdb.core.sqlobject.BaseColumn;
import org.verdictdb.core.sqlobject.BaseTable;
import org.verdictdb.core.sqlobject.ColumnOp;
import org.verdictdb.core.sqlobject.ConstantColumn;
import org.verdictdb.core.sqlobject.GroupingAttribute;
import org.verdictdb.core.sqlobject.OrderbyAttribute;
import org.verdictdb.core.sqlobject.SelectItem;
import org.verdictdb.core.sqlobject.SelectQuery;
import org.verdictdb.core.sqlobject.SqlConvertible;
import org.verdictdb.core.sqlobject.UnnamedColumn;
import org.verdictdb.exception.VerdictDBException;
import org.verdictdb.exception.VerdictDBValueException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * Represents an "progressive" execution of a single aggregate query (without nested components).
 *
 * <p>Steps: 1. identify agg and nonagg columns of a given select agg query. 2. convert the query
 * into multiple block-agg queries. 3. issue those block-agg queries one by one. 4. combine the
 * results of those block-agg queries as the answers to those queries arrive. 5. depending on the
 * interface, call an appropriate result handler.
 *
 * @author Yongjoo Park
 */
public class AsyncAggExecutionNode extends ProjectionNode {

  private static final long serialVersionUID = -1829554239432075523L;

  private static final String INNER_RAW_AGG_TABLE_ALIAS = "verdictdb_internal_before_scaling";

  private static final String TIER_CONSOLIDATED_TABLE_ALIAS =
      "verdictdb_internal_tier_consolidated";

  private static final String HAVING_CONDITION_ALIAS = "verdictdb_having_cond";

  private static final String GROUP_BY_ALIAS = "verdictdb_group_by";

  private static final String ORDER_BY_ALIAS = "verdictdb_order_by";

  //  private static final String SCRAMBLE_META_STORE_KEY = "scrambleMeta";

  private ScrambleMetaSet scrambleMeta;

  //  private String newTableSchemaName;
  //
  //  private String newTableName;

  // This is the list of aggregate columns contained in the selectQuery field.
  private List<ColumnOp> aggColumns;

  private Map<Integer, String> scrambledTableTierInfo;

  /**
   * This is the Map that maps the aggregation alias to its column contents. For example, if one
   * basic aggregate column in the aggregate node is sum(value) as agg0, it will record [agg0,
   * sum(value)].
   */
  HashMap<String, UnnamedColumn> aggContents = new HashMap<>();

  int tableNum = 1;

  private AsyncAggExecutionNode(IdCreator idCreator) {
    super(idCreator, null);
  }

  /**
   * A factory method for AsyncAggExecutionNode.
   *
   * <p>This static method performs the following operations: 1. Link individual aggregate nodes and
   * combiners appropriately. 2. Create a base table which includes several placeholders - scale
   * factor placeholder - temp table placeholder
   *
   * @param idCreator
   * @param aggblocks
   * @param combiners
   * @param meta
   * @param aggNodeBlock
   * @return
   */
  public static AsyncAggExecutionNode create(
      IdCreator idCreator,
      List<AggExecutionNodeBlock> aggblocks,
      List<ExecutableNodeBase> combiners,
      ScrambleMetaSet meta,
      AggExecutionNodeBlock aggNodeBlock) {

    AsyncAggExecutionNode node = new AsyncAggExecutionNode(idCreator);

    // this placeholder base table is used for query construction later
    String alias = INNER_RAW_AGG_TABLE_ALIAS;
    Pair<BaseTable, SubscriptionTicket> tableAndTicket = node.createPlaceHolderTable(alias);
    BaseTable placeholderTable = tableAndTicket.getLeft();
    SubscriptionTicket ticket = tableAndTicket.getRight();

    // first agg -> root
    AggExecutionNode firstSource = (AggExecutionNode) aggblocks.get(0).getBlockRootNode();
    firstSource.registerSubscriber(ticket);
    //    node.subscribeTo(individualAggs.get(0), 0);

    // combiners -> root
    for (ExecutableNodeBase c : combiners) {
      c.registerSubscriber(ticket);
      //      node.subscribeTo(c, 0);
    }
    // make the leaf nodes dependent on the aggroot of the previous iteration
    // this will make all the operations on the (i+1)-th block performed after the operations
    // on the i-th block.
    for (int i = 0; i < aggblocks.size(); i++) {
      if (i > 0) {
        AggExecutionNodeBlock aggblock = aggblocks.get(i);
        List<ExecutableNodeBase> leafNodes = aggblock.getLeafNodes();
        ExecutableNodeBase prevAggRoot = aggblocks.get(i - 1).getBlockRootNode();
        for (ExecutableNodeBase leaf : leafNodes) {
          leaf.subscribeTo(prevAggRoot, prevAggRoot.getId());
        }
      }
    }

    //    // agg -> next agg (to enfore the execution order)
    //    for (int i = 0; i < individualAggs.size()-1; i++) {
    //      AggExecutionNode thisNode = (AggExecutionNode) individualAggs.get(i);
    //      AggExecutionNode nextNode = (AggExecutionNode) individualAggs.get(i+1);
    //      int channel = thisNode.getId();
    //      nextNode.subscribeTo(thisNode, channel);
    //    }

    node.setScrambleMetaSet(meta); // the scramble meta must be not be shared; thus, thread-safe
    node.setNamer(idCreator); // the name can be shared

    // creates a base query that contain placeholders
    AggMeta sourceAggMeta = firstSource.getAggMeta();
    List<SelectItem> sourceSelectList = firstSource.getSelectQuery().getSelectList();
    Triple<List<ColumnOp>, SqlConvertible, Map<Integer, String>> aggColumnsAndQuery =
        createBaseQueryForReplacement(sourceAggMeta, sourceSelectList, placeholderTable, meta);
    node.aggColumns = aggColumnsAndQuery.getLeft();
    SelectQuery subquery = (SelectQuery) aggColumnsAndQuery.getMiddle();
    Pair<SelectQuery, HashMap<String, UnnamedColumn>> pair =
        sumUpTierGroup(subquery, sourceAggMeta);
    node.selectQuery = pair.getLeft();
    node.aggContents = pair.getRight();
    node.scrambledTableTierInfo =
        new ImmutableMap.Builder<Integer, String>().putAll(aggColumnsAndQuery.getRight()).build();

    // add (1) order-by, (2) limit, (3) having clauses to the select query
    QueryNodeBase aggRoot = (QueryNodeBase) aggNodeBlock.getBlockRootNode();
    SelectQuery originalAggQuery = aggRoot.getSelectQuery();

    //    int orderByCount = 0;
    //    for (OrderbyAttribute orderBy : originalAggQuery.getOrderby()) {
    //      String aliasName = ORDER_BY_ALIAS + (orderByCount++);
    //      //      if (orderBy.getAttribute() instanceof AliasedColumn) {
    //      //        aliasName = ((AliasedColumn) orderBy.getAttribute()).getAliasName();
    //      //      } else if (orderBy.getAttribute() instanceof AliasReference) {
    //      //        aliasName = ((AliasReference) orderBy.getAttribute()).getAliasName();
    //      //      } else {
    //      //        // TODO
    //      //        return null;
    //      //      }
    //      BaseColumn col = new BaseColumn(TIER_CONSOLIDATED_TABLE_ALIAS, aliasName);
    //      node.selectQuery.addOrderby(
    //          new OrderbyAttribute(col, orderBy.getOrder(), orderBy.getNullsOrder()));
    //    }

    node.selectQuery.addOrderby(originalAggQuery.getOrderby());

    //    int orderByCount = 0;
    //    for (SelectItem item : node.selectQuery.getSelectList()) {
    //      if (item instanceof AliasedColumn) {
    //        AliasedColumn ac = (AliasedColumn) item;
    //        if (ac.getAliasName().startsWith(AsyncAggExecutionNode.getOrderByAlias())) {
    //          OrderbyAttribute attr = originalAggQuery.getOrderby().get(orderByCount);
    //          BaseColumn col = new BaseColumn(TIER_CONSOLIDATED_TABLE_ALIAS, ac.getAliasName());
    //          node.selectQuery.addOrderby(
    //              new OrderbyAttribute(col, attr.getOrder(), attr.getNullsOrder()));
    //          ++orderByCount;
    //        }
    //      }
    //    }

    if (originalAggQuery.getLimit().isPresent()) {
      node.selectQuery.addLimit(originalAggQuery.getLimit().get());
    }
    if (originalAggQuery.getHaving().isPresent()) {
      node.selectQuery.addHavingByAnd(originalAggQuery.getHaving().get());
    }

    return node;
  }

  @Override
  public SqlConvertible createQuery(List<ExecutionInfoToken> tokens) throws VerdictDBException {
    //    super.createQuery(tokens);

    //    System.out.println("Starts the processing of AsyncAggNode.");
    //    System.out.println(selectQuery);

    ExecutionInfoToken token = tokens.get(0);
    AggMeta sourceAggMeta = (AggMeta) token.getValue("aggMeta");

    // First, calculate the scale factor and use it to replace the scale factor placeholder
    List<Pair<UnnamedColumn, Double>> conditionToScaleFactor =
        composeScaleFactorForTierCombinations(sourceAggMeta, INNER_RAW_AGG_TABLE_ALIAS);

    // update the agg column scaling factor
    List<UnnamedColumn> scalingOperands = new ArrayList<>();
    for (Pair<UnnamedColumn, Double> condToScale : conditionToScaleFactor) {
      UnnamedColumn cond = condToScale.getKey();
      double scale = condToScale.getValue();
      scalingOperands.add(cond);
      scalingOperands.add(ConstantColumn.valueOf(scale));
    }
    scalingOperands.add(ConstantColumn.valueOf(1.0)); // default scaling factor is always 1.0
    ColumnOp scalingColumn = ColumnOp.casewhen(scalingOperands);
    for (ColumnOp aggcol : aggColumns) {
      aggcol.setOperand(0, scalingColumn);
    }



    selectQuery = replaceWithOriginalSelectList(selectQuery, sourceAggMeta);

    //    System.out.println("Finished composing a query in AsyncAggNode.");
    //    System.out.println(selectQuery);

    return super.createQuery(tokens);
  }

  /**
   * Compose pairs of tier indicator and its associated scaling factor. The function generates the
   * operands needed for writing (by the caller) the following case-when clause: "case when cond1
   * then scale1 when cond2 else scale2 end" clause.
   *
   * @param sourceAggMeta The aggmeta of the downstream node
   * @return Map of a filtering condition to the scaling factor; the filtering conditions correspond
   *     to cond1, cond2, etc.; the scaling factors correspond to scale1, scale2, etc.
   */
  private List<Pair<UnnamedColumn, Double>> composeScaleFactorForTierCombinations(
      AggMeta sourceAggMeta, String sourceTableAlias) {
    List<Pair<UnnamedColumn, Double>> scalingFactorPerTier = new ArrayList<>();

    Map<TierCombination, Double> scaleFactors = sourceAggMeta.computeScaleFactors();
    Map<ScrambleMeta, String> tierColums = sourceAggMeta.getTierColumnForScramble();

    // each iteration of this loop generates a single condition-then part
    for (Entry<TierCombination, Double> tierScale : scaleFactors.entrySet()) {
      UnnamedColumn tierCombinationCondition = null;
      TierCombination combination = tierScale.getKey();
      double scale = tierScale.getValue();

      // each iteration of this loop generates a condition that must be AND-connected
      // to compose the condition for an entire tier combination.
      for (Entry<Pair<String, String>, Integer> perTable : combination) {
        Pair<String, String> table = perTable.getKey();
        Integer tier = perTable.getValue();
        String aliasName = findScrambleAlias(tierColums, table);

        UnnamedColumn part =
            ColumnOp.equal(
                new BaseColumn(sourceTableAlias, aliasName), ConstantColumn.valueOf(tier));

        if (tierCombinationCondition == null) {
          tierCombinationCondition = part;
        } else {
          tierCombinationCondition = ColumnOp.and(tierCombinationCondition, part);
        }
      }

      scalingFactorPerTier.add(new ImmutablePair<>(tierCombinationCondition, scale));
    }

    return scalingFactorPerTier;
  }

  private String findScrambleAlias(
      Map<ScrambleMeta, String> tierColums, Pair<String, String> table) {

    for (Entry<ScrambleMeta, String> metaToAlias : tierColums.entrySet()) {
      ScrambleMeta meta = metaToAlias.getKey();
      String aliasName = metaToAlias.getValue();
      if (meta.getSchemaName().equals(table.getLeft())
          && meta.getTableName().equals(table.getRight())) {
        return aliasName;
      }
    }
    return null;
  }

  @Override
  public ExecutionInfoToken createToken(DbmsQueryResult result) {
    ExecutionInfoToken token = super.createToken(result);
    return token;
  }

  public ScrambleMetaSet getScrambleMeta() {
    //    return (ScrambleMetaSet) retrieveStoredObjectThreadSafely(SCRAMBLE_META_STORE_KEY);
    return scrambleMeta;
  }

  public void setScrambleMetaSet(ScrambleMetaSet meta) {
    //    storeObjectThreadSafely(SCRAMBLE_META_STORE_KEY, meta);
    this.scrambleMeta = meta;
  }

  /**
   * @param sourceAggMeta AggMeta instance passed from a downstream node (either an individual
   *     aggregate node or a combiner node). This object contains what are the tier columns for the
   *     scrambled tables they cover and what aggregates are being computed.
   * @return Key: aggregate column list
   */
  private static Triple<List<ColumnOp>, SqlConvertible, Map<Integer, String>>
      createBaseQueryForReplacement(
          AggMeta sourceAggMeta,
          List<SelectItem> sourceSelectList,
          BaseTable placeholderTable,
          ScrambleMetaSet metaSet) {

    Map<Integer, String> multipleTierTableTierInfo = new HashMap<>();
    List<ColumnOp> aggColumnlist = new ArrayList<>();
    //    ScrambleMetaSet scrambleMetaSet = getScrambleMeta();

    List<HyperTableCube> cubes = sourceAggMeta.getCubes();
    //    SelectQuery dependentQuery = (SelectQuery) token.getValue("dependentQuery");

    //    dependentQuery.deepcopy().getSelectList();
    List<SelectItem> newSelectList = new ArrayList<>(sourceSelectList);
    //    AggMeta aggMeta = (AggMeta) token.getValue("aggMeta");

    for (SelectItem selectItem : newSelectList) {
      if (selectItem instanceof AliasedColumn) {
        AliasedColumn aliasedColumn = (AliasedColumn) selectItem;
        int index = newSelectList.indexOf(selectItem);
        UnnamedColumn col = aliasedColumn.getColumn();

        if (sourceAggMeta.getAggAlias().contains(aliasedColumn.getAliasName())) {
          ColumnOp aggColumn =
              ColumnOp.multiply(
                  ConstantColumn.valueOf(1.0),
                  new BaseColumn(INNER_RAW_AGG_TABLE_ALIAS, aliasedColumn.getAliasName()));
          aggColumnlist.add(aggColumn);
          newSelectList.set(index, new AliasedColumn(aggColumn, aliasedColumn.getAliasName()));
        } else if (sourceAggMeta
            .getMaxminAggAlias()
            .keySet()
            .contains(aliasedColumn.getAliasName())) {
          newSelectList.set(
              index,
              new AliasedColumn(
                  new BaseColumn(INNER_RAW_AGG_TABLE_ALIAS, aliasedColumn.getAliasName()),
                  aliasedColumn.getAliasName()));
        } else {
          // Looking for tier column
          //          if (!Initiated && col instanceof BaseColumn) {
          if (col instanceof BaseColumn) {
            String schemaName = ((BaseColumn) col).getSchemaName();
            String tableName = ((BaseColumn) col).getTableName();
            if (metaSet.isScrambled(schemaName, tableName)
                && ((BaseColumn) col)
                    .getColumnName()
                    .equals(metaSet.getTierColumn(schemaName, tableName))) {
              for (Dimension d : cubes.get(0).getDimensions()) {
                if (d.getTableName().equals(tableName) && d.getSchemaName().equals(schemaName)) {
                  multipleTierTableTierInfo.put(
                      cubes.get(0).getDimensions().indexOf(d), aliasedColumn.getAliasName());
                  break;
                }
              }
            }
          }

          newSelectList.set(
              index,
              new AliasedColumn(
                  new BaseColumn(INNER_RAW_AGG_TABLE_ALIAS, aliasedColumn.getAliasName()),
                  aliasedColumn.getAliasName()));
        }
      }
    }

    // Setup from table
    SelectQuery query = SelectQuery.create(newSelectList, placeholderTable);

    return Triple.of(aggColumnlist, (SqlConvertible) query, multipleTierTableTierInfo);
  }

  /**
   * Currently, assume block size is uniform
   *
   * @return Return tier permutation list and its scale factor
   */
  private HashMap<List<Integer>, Double> calculateScaleFactor(
      AggMeta sourceAggMeta, Map<Integer, String> multipleTierTableTierInfo)
      throws VerdictDBValueException {

    List<HyperTableCube> cubes = sourceAggMeta.getCubes();
    ScrambleMetaSet scrambleMetaSet = this.getScrambleMeta();
    List<ScrambleMeta> metaForTablesList = new ArrayList<>();
    List<Integer> blockCountList = new ArrayList<>();
    List<Pair<Integer, Integer>> scrambleTableTierInfo =
        new ArrayList<>(); // block span index for each table

    for (Dimension d : cubes.get(0).getDimensions()) {
      ScrambleMeta scrambleMeta =
          scrambleMetaSet.getSingleMeta(d.getSchemaName(), d.getTableName());

      blockCountList.add(
          scrambleMetaSet.getAggregationBlockCount(d.getSchemaName(), d.getTableName()));
      metaForTablesList.add(scrambleMeta);
      scrambleTableTierInfo.add(
          new ImmutablePair<>(
              cubes.get(0).getDimensions().indexOf(d),
              scrambleMetaSet
                  .getSingleMeta(d.getSchemaName(), d.getTableName())
                  .getNumberOfTiers()));

      //      if (scrambleMeta.getNumberOfTiers() > 1 && !Initiated) {
      if (scrambleMeta.getNumberOfTiers() > 1) {
        //        ScrambleMeta meta = scrambleMetaSet.getSingleMeta(d.getSchemaName(),
        // d.getTableName());
        Map<ScrambleMeta, String> scrambleTableTierColumnAlias =
            sourceAggMeta.getTierColumnForScramble();

        if (scrambleTableTierColumnAlias.containsKey(scrambleMeta) == false) {
          throw new VerdictDBValueException("The metadata for a scrambled table is not found.");
        }

        // TODO: move this somewhere
        //        multipleTierTableTierInfo.put(
        //            cubes.get(0).getDimensions().indexOf(d),
        //            tierColumnForScramble.get(scrambleMeta));

        //        if (tierColumnForScramble.containsKey(meta)) {
        //
        //        }
        //        else {
        //          multipleTierTableTierInfo.put(
        //              cubes.get(0).getDimensions().indexOf(d),
        //              scrambleMeta.getSingleMeta(d.getSchemaName(),
        // d.getTableName()).getTierColumn());
        //        }
      }
    }

    List<List<Integer>> tierPermuation = generateTierPermuation(scrambleTableTierInfo);
    HashMap<List<Integer>, Double> scaleFactor = new HashMap<>();
    for (List<Integer> tierlist : tierPermuation) {
      double total = 0;
      for (HyperTableCube cube : cubes) {
        double scale = 1;
        for (int i = 0; i < tierlist.size(); i++) {
          int tier = tierlist.get(i);
          Dimension d = cube.getDimensions().get(i);

          double prob;
          if (d.getBegin() == 0) {
            prob = metaForTablesList.get(i).getCumulativeDistributionForTier(tier).get(d.getEnd());
          } else {
            prob =
                metaForTablesList.get(i).getCumulativeDistributionForTier(tier).get(d.getEnd())
                    - metaForTablesList
                        .get(i)
                        .getCumulativeDistributionForTier(tier)
                        .get(d.getBegin() - 1);
          }

          scale = scale * prob;
        }
        total += scale;
      }

      if (total == 0) {
        scaleFactor.put(tierlist, 0.0);
      } else {
        scaleFactor.put(tierlist, 1.0 / total);
      }
    }
    return scaleFactor;
  }

  /**
   * Generate the permuation of multiple tiers
   *
   * @param scrambleTableTierInfo
   * @return
   */
  private List<List<Integer>> generateTierPermuation(
      List<Pair<Integer, Integer>> scrambleTableTierInfo) {
    if (scrambleTableTierInfo.size() == 1) {
      List<List<Integer>> res = new ArrayList<>();
      for (int tier = 0; tier < scrambleTableTierInfo.get(0).getRight(); tier++) {
        res.add(Arrays.asList(tier));
      }
      return res;
    } else {
      List<Pair<Integer, Integer>> next =
          scrambleTableTierInfo.subList(1, scrambleTableTierInfo.size());
      List<List<Integer>> subres = generateTierPermuation(next);
      List<List<Integer>> res = new ArrayList<>();
      for (int tier = 0; tier < scrambleTableTierInfo.get(0).getRight(); tier++) {
        for (List<Integer> tierlist : subres) {
          List<Integer> newTierlist = new ArrayList<>();
          for (int i : tierlist) {
            newTierlist.add(Integer.valueOf(i));
          }
          newTierlist.add(0, tier);
          res.add(newTierlist);
        }
      }
      return res;
    }
  }

  private UnnamedColumn generateCaseCondition(
      List<Integer> tierlist, Map<Integer, String> multipleTierTableTierInfo) {

    Optional<ColumnOp> col = Optional.absent();
    for (Map.Entry<Integer, String> entry : multipleTierTableTierInfo.entrySet()) {
      BaseColumn tierColumn = new BaseColumn(INNER_RAW_AGG_TABLE_ALIAS, entry.getValue());
      ColumnOp equation =
          new ColumnOp(
              "equal",
              Arrays.asList(tierColumn, ConstantColumn.valueOf(tierlist.get(entry.getKey()))));
      if (col.isPresent()) {
        col = Optional.of(new ColumnOp("and", Arrays.<UnnamedColumn>asList(equation, col.get())));
      } else {
        col = Optional.of(equation);
      }
    }
    return col.get();
  }

  private SelectItem replaceColumnWithAggMeta(SelectItem sel, AggMeta aggMeta) {

    Map<SelectItem, List<ColumnOp>> aggColumn = aggMeta.getAggColumn();
    // Case 1: aggregate column
    if (aggColumn.containsKey(sel)) {
      List<ColumnOp> columnOps = aggColumn.get(sel);
      for (ColumnOp col : columnOps) {
        // If it is count or sum, set col to be aggContents
        if (col.getOpType().equals("count") || col.getOpType().equals("sum")) {
          String aliasName = null;
          if (col.getOpType().equals("count")) {
            aliasName =
                aggMeta
                    .getAggColumnAggAliasPair()
                    .get(
                        new ImmutablePair<>(col.getOpType(), (UnnamedColumn) new AsteriskColumn()));
            if (aliasName==null) {
              aliasName =
                  aggMeta
                      .getAggColumnAggAliasPair()
                      .get(new ImmutablePair<>(col.getOpType(), col.getOperand(0)));
            }
          } else {
            aliasName =
                aggMeta
                    .getAggColumnAggAliasPair()
                    .get(new ImmutablePair<>(col.getOpType(), col.getOperand(0)));
          }
          ColumnOp aggContent = (ColumnOp) aggContents.get(aliasName);
          col.setOpType(aggContent.getOpType());
          col.setOperand(aggContent.getOperands());
        } else if (col.getOpType().equals("max") || col.getOpType().equals("min")) {
          String aliasName =
              aggMeta
                  .getAggColumnAggAliasPairOfMaxMin()
                  .get(new ImmutablePair<>(col.getOpType(), col.getOperand(0)));
          if (aggContents.get(aliasName) instanceof BaseColumn) {
            BaseColumn aggContent = (BaseColumn) aggContents.get(aliasName);
            col.setOpType("multiply");
            col.setOperand(Arrays.asList(ConstantColumn.valueOf(1), aggContent));
          } else {
            ColumnOp aggContent = (ColumnOp) aggContents.get(aliasName);
            col.setOpType(aggContent.getOpType());
            col.setOperand(aggContent.getOperands());
          }
        }
        // If it is avg, set col to be divide columnOp
        else if (col.getOpType().equals("avg")) {
          String aliasNameSum =
              aggMeta.getAggColumnAggAliasPair().get(new ImmutablePair<>("sum", col.getOperand(0)));
          ColumnOp aggContentSum = (ColumnOp) aggContents.get(aliasNameSum);
          String aliasNameCount =
              aggMeta
                  .getAggColumnAggAliasPair()
                  .get(new ImmutablePair<>("count", (UnnamedColumn) new AsteriskColumn()));
          ColumnOp aggContentCount = (ColumnOp) aggContents.get(aliasNameCount);
          col.setOpType("divide");
          col.setOperand(Arrays.<UnnamedColumn>asList(aggContentSum, aggContentCount));
        }
      }
    }
    // Case 2: non-aggregate column
    // this non-aggregate column must exist in the column list of the scaled table
    else if (sel instanceof AliasedColumn) {
      //      AliasedColumn ac = (AliasedColumn) sel;
      //      if (ac.getAliasName().startsWith(AsyncAggExecutionNode.getHavingConditionAlias())
      //          || ac.getAliasName().startsWith(AsyncAggExecutionNode.getGroupByAlias())
      //          || ac.getAliasName().startsWith(AsyncAggExecutionNode.getOrderByAlias())) {
      //        return null;
      //      } else {
      ((AliasedColumn) sel)
          .setColumn(
              new BaseColumn(TIER_CONSOLIDATED_TABLE_ALIAS, ((AliasedColumn) sel).getAliasName()));
      ((AliasedColumn) sel).setAliasName(((AliasedColumn) sel).getAliasName());
      //      }
    }
    return sel;
  }

  /**
   * Replace the scaled select list with original select list
   *
   * @param queryToReplace, aggMeta
   * @return replaced original select list
   */
  private SelectQuery replaceWithOriginalSelectList(SelectQuery queryToReplace, AggMeta aggMeta) {
    List<SelectItem> originalSelectList = aggMeta.getOriginalSelectList();
    List<SelectItem> newSelectList = new ArrayList<>();
    Map<SelectItem, List<ColumnOp>> aggColumn = aggMeta.getAggColumn();
    /*  HashMap<String, UnnamedColumn> aggContents = new HashMap<>();
    for (SelectItem sel : queryToReplace.getSelectList()) {
      // this column is a basic aggregate column
      if (sel instanceof AliasedColumn
          && aggMeta.getAggAlias().contains(((AliasedColumn) sel).getAliasName())) {
        aggContents.put(((AliasedColumn) sel).getAliasName(), ((AliasedColumn) sel).getColumn());
      } else if (sel instanceof AliasedColumn
          && aggMeta.getMaxminAggAlias().keySet().contains(((AliasedColumn) sel).getAliasName())) {
        aggContents.put(((AliasedColumn) sel).getAliasName(), ((AliasedColumn) sel).getColumn());
      }
    }*/

    boolean firstHaving = true;
    int orderByIndex = 0;
    for (SelectItem sel : originalSelectList) {
      SelectItem replacedSel = this.replaceColumnWithAggMeta(sel, aggMeta);
      SelectItem item = (replacedSel != null) ? replacedSel : sel;
      if (item instanceof AliasedColumn) {
        AliasedColumn ac = (AliasedColumn) item;
        if (ac.getAliasName().startsWith(HAVING_CONDITION_ALIAS)) {
          if (firstHaving) {
            queryToReplace.clearHaving();
            firstHaving = false;
          }
          queryToReplace.addHavingByAnd(ac.getColumn());
        } else if (ac.getAliasName().startsWith(ORDER_BY_ALIAS)) {
          OrderbyAttribute attribute = queryToReplace.getOrderby().get(orderByIndex++);
          attribute.setAttribute(ac.getColumn());
        }
        if (ac.getAliasName().startsWith(HAVING_CONDITION_ALIAS)
            || ac.getAliasName().startsWith(ORDER_BY_ALIAS)
            || ac.getAliasName().startsWith(GROUP_BY_ALIAS)) {
          continue;
        }
      }
      newSelectList.add(item);
    }
    queryToReplace.clearSelectList();
    //    queryToReplace.getSelectList().addAll(originalSelectList);
    queryToReplace.getSelectList().addAll(newSelectList);
    return queryToReplace;
  }

  /**
   * Create a sum-up query that sum the results from all tier permutations
   *
   * @param subquery
   * @return select a pair that key is the query that sum all the tier results and value is the
   *     aggContents that record the aggregation column alias and the column itself
   */
  private static Pair<SelectQuery, HashMap<String, UnnamedColumn>> sumUpTierGroup(
      SelectQuery subquery, AggMeta sourceAggMeta) {

    List<String> aggAlias = sourceAggMeta.getAggAlias();
    Set<String> tierColumnAliases = sourceAggMeta.getAllTierColumnAliases();
    HashMap<String, UnnamedColumn> aggContents = new HashMap<>();

    List<GroupingAttribute> groupby = new ArrayList<>();
    List<SelectItem> newSelectlist = new ArrayList<>();
    for (SelectItem sel : subquery.getSelectList()) {
      if (sel instanceof AliasedColumn) {
        // If this is a basic aggregation, we need to sum up
        if (aggAlias.contains(((AliasedColumn) sel).getAliasName())) {
          UnnamedColumn col =
              ColumnOp.sum(
                  new BaseColumn(
                      TIER_CONSOLIDATED_TABLE_ALIAS, ((AliasedColumn) sel).getAliasName()));
          newSelectlist.add(new AliasedColumn(col, ((AliasedColumn) sel).getAliasName()));
          aggContents.put(((AliasedColumn) sel).getAliasName(), col);
          String alias = ((AliasedColumn) sel).getAliasName();

          // Add the item to group-by if it is group-by column
          if (alias.startsWith(GROUP_BY_ALIAS)) {
            UnnamedColumn newcol = new BaseColumn(TIER_CONSOLIDATED_TABLE_ALIAS, alias);
            groupby.add(newcol);
          }
        }
        // If it is a max/min aggregation, we need to maximize/minimize
        else if (sourceAggMeta
            .getMaxminAggAlias()
            .keySet()
            .contains(((AliasedColumn) sel).getAliasName())) {
          String opType =
              sourceAggMeta.getMaxminAggAlias().get(((AliasedColumn) sel).getAliasName());
          UnnamedColumn col =
              new ColumnOp(
                  opType,
                  new BaseColumn(
                      TIER_CONSOLIDATED_TABLE_ALIAS, ((AliasedColumn) sel).getAliasName()));
          newSelectlist.add(new AliasedColumn(col, ((AliasedColumn) sel).getAliasName()));
          aggContents.put(((AliasedColumn) sel).getAliasName(), col);
        } else {
          // if it is not a tier column, we need to put it in the group by list
          if (!tierColumnAliases.contains(((AliasedColumn) sel).getAliasName())) {
            UnnamedColumn newcol =
                new BaseColumn(TIER_CONSOLIDATED_TABLE_ALIAS, ((AliasedColumn) sel).getAliasName());
            groupby.add(newcol);
            AliasedColumn ac = (AliasedColumn) sel;
            if (ac.getAliasName().startsWith(AsyncAggExecutionNode.getHavingConditionAlias())
                || ac.getAliasName().startsWith(AsyncAggExecutionNode.getGroupByAlias())
                || ac.getAliasName().startsWith(AsyncAggExecutionNode.getOrderByAlias())) {
              continue;
            }
            newSelectlist.add(new AliasedColumn(newcol, ((AliasedColumn) sel).getAliasName()));
          }
        }
      }
    }
    subquery.setAliasName(TIER_CONSOLIDATED_TABLE_ALIAS);
    SelectQuery query = SelectQuery.create(newSelectlist, subquery);
    for (GroupingAttribute group : groupby) {
      query.addGroupby(group);
    }
    return new ImmutablePair<>(query, aggContents);
  }

  public static String getHavingConditionAlias() {
    return HAVING_CONDITION_ALIAS;
  }

  public static String getGroupByAlias() {
    return GROUP_BY_ALIAS;
  }

  public static String getOrderByAlias() {
    return ORDER_BY_ALIAS;
  }

  @Override
  public ExecutableNodeBase deepcopy() {
    AsyncAggExecutionNode copy = new AsyncAggExecutionNode(getNamer());
    copyFields(this, copy);
    return copy;
  }

  void copyFields(AsyncAggExecutionNode from, AsyncAggExecutionNode to) {
    //    to.scrambleMeta = from.scrambleMeta;
    //    to.nonaggColumns = from.nonaggColumns;
    //    to.aggColumns = from.aggColumns;
  }
}
