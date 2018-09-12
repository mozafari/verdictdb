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

package org.verdictdb.core.scrambling;

import static org.verdictdb.core.scrambling.ScramblingNode.computeConditionalProbabilityDistribution;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;
import org.verdictdb.commons.DataTypeConverter;
import org.verdictdb.commons.VerdictDBLogger;
import org.verdictdb.connection.DbmsQueryResult;
import org.verdictdb.core.execplan.ExecutionInfoToken;
import org.verdictdb.core.querying.CreateTableAsSelectNode;
import org.verdictdb.core.querying.ExecutableNodeBase;
import org.verdictdb.core.querying.IdCreator;
import org.verdictdb.core.querying.QueryNodeBase;
import org.verdictdb.core.querying.QueryNodeWithPlaceHolders;
import org.verdictdb.core.querying.SubscriptionTicket;
import org.verdictdb.core.querying.TempIdCreatorInScratchpadSchema;
import org.verdictdb.core.sqlobject.AbstractRelation;
import org.verdictdb.core.sqlobject.AliasReference;
import org.verdictdb.core.sqlobject.AliasedColumn;
import org.verdictdb.core.sqlobject.BaseColumn;
import org.verdictdb.core.sqlobject.BaseTable;
import org.verdictdb.core.sqlobject.ColumnOp;
import org.verdictdb.core.sqlobject.ConstantColumn;
import org.verdictdb.core.sqlobject.JoinTable;
import org.verdictdb.core.sqlobject.SelectItem;
import org.verdictdb.core.sqlobject.SelectQuery;
import org.verdictdb.core.sqlobject.SqlConvertible;
import org.verdictdb.core.sqlobject.UnnamedColumn;
import org.verdictdb.exception.VerdictDBException;
import org.verdictdb.exception.VerdictDBValueException;

import com.google.common.base.Optional;

/**
 * Policy: 1. Tier 0: tuples containing outlier values. 2. Tier 1: tuples containing rare groups 3.
 * Tier 2: other tuples
 *
 * <p>Outlier values: Bottom 0.1% and 99.9% percentile values for every numeric column. Suppose a
 * table has 20 numeric columns; then, in the worst case, 20 * 0.2% = 4% of the tuples are the
 * tuples containing outlier values.
 *
 * <p>Blocks for Tier 0: Tier 0 takes up to 50% of each block.
 *
 * <p>Blocks for Tier 1: Tier 0 + Tier 1 take up to 80% of each block.
 *
 * <p>Blocks for Tier 2: Tier 2 takes the rest of the space in each block.
 *
 * @author Yongjoo Park
 */
public class FastConvergeScramblingMethod extends ScramblingMethodBase {

  double p0 = 0.5; // max portion for Tier 0; should be configured dynamically in the future

  double p1 =
      0.8; // max portion for Tier 0 + Tier 1; should be configured dynamically in the future

  static final double OUTLIER_STDDEV_MULTIPLIER = 3.09;

  Optional<String> primaryColumnName = Optional.absent();

  private String scratchpadSchemaName;

  List<Double> tier0CumulProbDist = null;

  List<Double> tier1CumulProbDist = null;

  List<Double> tier2CumulProbDist = null;

  private long outlierSize = 0; // tier 0 size

  private long smallGroupSizeSum = 0;

  public static final String MAIN_TABLE_SOURCE_ALIAS_NAME = "t1";

  public static final String RIGHT_TABLE_SOURCE_ALIAS_NAME = "t2";

  private int totalNumberOfblocks = -1;
  
  private static VerdictDBLogger log = VerdictDBLogger.getLogger(FastConvergeScramblingMethod.class);

  public FastConvergeScramblingMethod(long blockSize, String scratchpadSchemaName) {
    super(blockSize);
    this.scratchpadSchemaName = scratchpadSchemaName;
  }

  public FastConvergeScramblingMethod(
      long blockSize, String scratchpadSchemaName, String primaryColumnName) {
    this(blockSize, scratchpadSchemaName);
    this.primaryColumnName = Optional.of(primaryColumnName);
  }

  /**
   * Computes three nodes. They compute: (1) 0.1% and 99.9% percentiles of numeric columns and the
   * total count, (for this, we compute standard deviations and estimate those percentiles based on
   * the standard deviations and normal distribution assumptions. \pm 3.09 * stddev is the 99.9 and
   * 0.1 percentiles of the standard normal distribution.) (2) the list of "large" groups, and (3)
   * the sizes of "large" groups
   *
   * <p>Recall that channels 100 and 101 are reserved for column meta and partition meta,
   * respectively.
   *
   * <p>This method generates up to three nodes, and the token keys set up by those nodes are: 1.
   * queryResult: this contains avg, std, and count 2. schemaName, tableName: this is the name of
   * the temporary tables that contains a list of large groups. 3. queryResult: this contains the
   * sum of the sizes of large groups.
   */
  @Override
  public List<ExecutableNodeBase> getStatisticsNode(
      String oldSchemaName,
      String oldTableName,
      String columnMetaTokenKey,
      String partitionMetaTokenKey) {

    List<ExecutableNodeBase> statisticsNodes = new ArrayList<>();

    // outlier checking
    PercentilesAndCountNode pc =
        new PercentilesAndCountNode(
            oldSchemaName,
            oldTableName,
            columnMetaTokenKey,
            partitionMetaTokenKey,
            primaryColumnName);
    statisticsNodes.add(pc);

    // outlier proportion computation
    OutlierProportionNode op = new OutlierProportionNode(oldSchemaName, oldTableName);
    op.subscribeTo(pc);
    statisticsNodes.add(op);

    // primary group's distribution checking
    if (primaryColumnName.isPresent()) {
      TempIdCreatorInScratchpadSchema idCreator =
          new TempIdCreatorInScratchpadSchema(scratchpadSchemaName);
      LargeGroupListNode ll =
          new LargeGroupListNode(
              idCreator, oldSchemaName, oldTableName, primaryColumnName.get(), blockSize);
      // subscribed to 'pc' to obtain count(*) of the table, which is used to infer
      // appropriate sampling ratio.
      ll.subscribeTo(pc, 0);

      LargeGroupSizeNode ls = new LargeGroupSizeNode(primaryColumnName.get());
      ll.registerSubscriber(ls.getSubscriptionTicket());
//      ls.subscribeTo(ll, 0);

      statisticsNodes.add(ll);
      statisticsNodes.add(ls);
    }

    return statisticsNodes;
  }
  
  static UnnamedColumn createOutlierTuplePredicate(
      DbmsQueryResult percentileAndCountResult, String sourceTableAlias) {
    boolean printLog = false;
    return createOutlierTuplePredicate(percentileAndCountResult, sourceTableAlias, printLog);
  }
  
  static UnnamedColumn createOutlierTuplePredicate(
      DbmsQueryResult percentileAndCountResult, String sourceTableAlias, boolean printLog) {
    UnnamedColumn outlierPredicate = null;

    percentileAndCountResult.rewind();
    percentileAndCountResult.next(); // assumes that the original table has at least one row.

    for (int i = 0; i < percentileAndCountResult.getColumnCount(); i++) {
      String columnName = percentileAndCountResult.getColumnName(i);
      if (columnName.startsWith(PercentilesAndCountNode.AVG_PREFIX)) {
        String originalColumnName =
            columnName.substring(PercentilesAndCountNode.AVG_PREFIX.length());

        // this implementation assumes that corresponding stddev column comes right after
        // the current column indexed by 'i'.
        double columnAverage = percentileAndCountResult.getDouble(i);
        double columnStddev = percentileAndCountResult.getDouble(i + 1);
        double lowCriteria = columnAverage - columnStddev * OUTLIER_STDDEV_MULTIPLIER;
        double highCriteria = columnAverage + columnStddev * OUTLIER_STDDEV_MULTIPLIER;
        
        if (printLog) {
          log.info(String.format("In column %s, the values outside (%.2f,%.2f) "
              + "will be prioritized in future query processing.",
              columnName, lowCriteria, highCriteria));
        }

        UnnamedColumn newOrPredicate =
            ColumnOp.or(
                ColumnOp.less(
                    new BaseColumn(sourceTableAlias, originalColumnName),
                    ConstantColumn.valueOf(lowCriteria)),
                ColumnOp.greater(
                    new BaseColumn(sourceTableAlias, originalColumnName),
                    ConstantColumn.valueOf(highCriteria)));

        if (outlierPredicate == null) {
          outlierPredicate = newOrPredicate;
        } else {
          outlierPredicate = ColumnOp.or(outlierPredicate, newOrPredicate);
        }
      }
      if (columnName.equals(PercentilesAndCountNode.TOTAL_COUNT_ALIAS_NAME)) {
        // do nothing
      }
    }

    return outlierPredicate;
  }

  @Override
  public List<UnnamedColumn> getTierExpressions(Map<String, Object> metaData) {
    DbmsQueryResult percentileAndCountResult =
        (DbmsQueryResult) metaData.get(PercentilesAndCountNode.class.getSimpleName());
    //    String largeGroupListSchemaName = (String) metaData.get("1schemaName");
    //    String largeGroupListTableName = (String) metaData.get("1tableName");

    // Tier 0
    UnnamedColumn tier0Predicate =
        createOutlierTuplePredicate(
            percentileAndCountResult, FastConvergeScramblingMethod.MAIN_TABLE_SOURCE_ALIAS_NAME);

    // Tier 1
    // select (case ... when t2.groupSize is null then 1 else 2 end) as verdictdbtier
    // from schemaName.tableName t1 left join largeGroupSchema.largeGroupTable t2
    //   on t1.primaryGroup = t2.primaryGroup
    UnnamedColumn tier1Predicate;
    if (!primaryColumnName.isPresent()) {
      tier1Predicate = ColumnOp.equal(ConstantColumn.valueOf(0), ConstantColumn.valueOf(1));
    } else {
      String rightTableSourceAlias = FastConvergeScramblingMethod.RIGHT_TABLE_SOURCE_ALIAS_NAME;
      tier1Predicate =
          ColumnOp.rightisnull(
              new BaseColumn(rightTableSourceAlias, LargeGroupListNode.PRIMARY_GROUP_RENAME));
    }

    // Tier 2: automatically handled by this function's caller

    return Arrays.asList(tier0Predicate, tier1Predicate);
  }

  @Override
  public List<Double> getCumulativeProbabilityDistributionForTier(
      Map<String, Object> metaData, int tier) {
    // this cumulative prob is calculated at the first call of this method
    if (tier0CumulProbDist == null) {
      populateAllCumulativeProbabilityDistribution(metaData);
    }

    List<Double> dist;

    if (tier == 0) {
      dist = tier0CumulProbDist;
    } else if (tier == 1) {
      dist = tier1CumulProbDist;
    } else {
      // expected tier == 2
      dist = tier2CumulProbDist;
    }

    storeCumulativeProbabilityDistribution(tier, dist);
    return dist;
  }

  private void populateAllCumulativeProbabilityDistribution(Map<String, Object> metaData) {
    populateTier0CumulProbDist(metaData);
    populateTier1CumulProbDist(metaData);
    populateTier2CumulProbDist(metaData);
  }
  
  private long calcuteEvenBlockSize(int totalNumberOfblocks, long tableSize) {
    return (long) Math.round((float) tableSize / (float) totalNumberOfblocks);
  }

  private void populateTier0CumulProbDist(Map<String, Object> metaData) {
    List<Double> cumulProbDist = new ArrayList<>();

    // calculate the number of blocks
    Pair<Long, Integer> tableSizeAndBlockNumber = retrieveTableSizeAndBlockNumber(metaData);
    long tableSize = tableSizeAndBlockNumber.getLeft();
    int totalNumberOfblocks = tableSizeAndBlockNumber.getRight();
    long evenBlockSize = calcuteEvenBlockSize(totalNumberOfblocks, tableSize);

    DbmsQueryResult outlierProportion =
        (DbmsQueryResult) metaData.get(OutlierProportionNode.class.getSimpleName());
    outlierProportion.rewind();
    outlierProportion.next();
    outlierSize = outlierProportion.getLong(0);

    if (outlierSize * 2 >= tableSize) {
      // too large outlier -> no special treatment
      for (int i = 0; i < totalNumberOfblocks; i++) {
        cumulProbDist.add((i + 1) / (double) totalNumberOfblocks);
      }

    } else {
      Long remainingSize = outlierSize;

      while (outlierSize > 0 && remainingSize > 0) {
        // fill only p0 portion of each block at most
        if (remainingSize <= p0 * evenBlockSize) {
          cumulProbDist.add(1.0);
          break;
        } else {
          long thisBlockSize = (long) (evenBlockSize * p0);
          double ratio = thisBlockSize / (float) outlierSize;
          if (cumulProbDist.size() == 0) {
            cumulProbDist.add(ratio);
          } else {
            cumulProbDist.add(cumulProbDist.get(cumulProbDist.size() - 1) + ratio);
          }
          remainingSize -= thisBlockSize;
        }
      }

      // in case where the length of the prob distribution is not equal to the total block number
      while (cumulProbDist.size() < totalNumberOfblocks) {
        cumulProbDist.add(1.0);
      }
    }

    tier0CumulProbDist = cumulProbDist;
  }

  /**
   * Probability distribution for Tier1: small-group tier
   *
   * @param metaData
   */
  private void populateTier1CumulProbDist(Map<String, Object> metaData) {
    List<Double> cumulProbDist = new ArrayList<>();

    // calculate the number of blocks
    Pair<Long, Integer> tableSizeAndBlockNumber = retrieveTableSizeAndBlockNumber(metaData);
    long tableSize = tableSizeAndBlockNumber.getLeft();
    int totalNumberOfblocks = tableSizeAndBlockNumber.getRight();
    long evenBlockSize = calcuteEvenBlockSize(totalNumberOfblocks, tableSize);

    if (!primaryColumnName.isPresent()) {
      while (cumulProbDist.size() < totalNumberOfblocks) {
        cumulProbDist.add(1.0);
      }
    } else {
      // obtain total large group size
      DbmsQueryResult largeGroupSizeResult =
          (DbmsQueryResult) metaData.get(LargeGroupSizeNode.class.getSimpleName());
      largeGroupSizeResult.rewind();
      largeGroupSizeResult.next();
      long largeGroupSizeSum = largeGroupSizeResult.getLong(0);
      smallGroupSizeSum = tableSize - largeGroupSizeSum;

      // count the remaining size (after tier0)
      long totalRemainingSizeUnderP1 = 0;
      for (int i = 0; i < tier0CumulProbDist.size(); i++) {
        if (i == 0) {
          totalRemainingSizeUnderP1 += evenBlockSize * p1 - outlierSize * tier0CumulProbDist.get(i);
        } else {
          totalRemainingSizeUnderP1 +=
              evenBlockSize * p1
                  - outlierSize * (tier0CumulProbDist.get(i) - tier0CumulProbDist.get(i - 1));
        }
      }

      // In this extreme case (i.e., the sum of the sizes of small groups is too big),
      // we simply uniformly distribute them.
      if (smallGroupSizeSum >= totalRemainingSizeUnderP1) {
        for (int i = 0; i < totalNumberOfblocks; i++) {
          cumulProbDist.add((i + 1) / (double) totalNumberOfblocks);
        }
      }
      // Otherwise, we fill up to (p1 - p0) portion within each block.
      else {
        long remainingSize = smallGroupSizeSum;

        while (remainingSize > 0) {
          if (remainingSize <= (p1 - p0) * evenBlockSize) {
            cumulProbDist.add(1.0);
            break;
          } else {
            long thisBlockSize = (long) (evenBlockSize * (p1 - p0));
            double ratio = thisBlockSize / smallGroupSizeSum;
            if (cumulProbDist.size() == 0) {
              cumulProbDist.add(ratio);
            } else {
              cumulProbDist.add(cumulProbDist.get(cumulProbDist.size() - 1) + ratio);
            }
            remainingSize -= thisBlockSize;
          }
        }

        // in case where the length of the prob distribution is not equal to the total block number
        while (cumulProbDist.size() < totalNumberOfblocks) {
          cumulProbDist.add(1.0);
        }
      }
    }

    tier1CumulProbDist = cumulProbDist;
  }

  /**
   * Probability distribution for Tier2: the tuples that do not belong to tier0 or tier1.
   *
   * @param metaData
   */
  private void populateTier2CumulProbDist(Map<String, Object> metaData) {
    List<Double> cumulProbDist = new ArrayList<>();

    // calculate the number of blocks
    Pair<Long, Integer> tableSizeAndBlockNumber = retrieveTableSizeAndBlockNumber(metaData);
    long tableSize = tableSizeAndBlockNumber.getLeft();
    int totalNumberOfblocks = tableSizeAndBlockNumber.getRight();
    long evenBlockSize = calcuteEvenBlockSize(totalNumberOfblocks, tableSize);

    //    System.out.println("table size: " + tableSize);
    //    System.out.println("outlier size: " + outlierSize);
    //    System.out.println("small group size: " + smallGroupSizeSum);
    long tier2Size = tableSize - outlierSize - smallGroupSizeSum;

    for (int i = 0; i < totalNumberOfblocks; i++) {
      long thisTier0Size;
      long thisTier1Size;
      if (i == 0) {
        thisTier0Size = (long) (outlierSize * tier0CumulProbDist.get(i));
        thisTier1Size = (long) (smallGroupSizeSum * tier1CumulProbDist.get(i));
      } else {
        thisTier0Size =
            (long) (outlierSize * (tier0CumulProbDist.get(i) - tier0CumulProbDist.get(i - 1)));
        thisTier1Size =
            (long)
                (smallGroupSizeSum * (tier1CumulProbDist.get(i) - tier1CumulProbDist.get(i - 1)));
      }
      long thisBlockSize = evenBlockSize - thisTier0Size - thisTier1Size;
      if (tier2Size == 0) {
        cumulProbDist.add(1.0);
      } else {
        double thisBlockRatio = thisBlockSize / (double) tier2Size;
        if (i == 0) {
          cumulProbDist.add(thisBlockRatio);
        } else if (i == totalNumberOfblocks - 1) {
          cumulProbDist.add(1.0);
        } else {
          cumulProbDist.add(Math.min(cumulProbDist.get(i - 1) + thisBlockRatio, 1.0));
        }
      }
    }

    tier2CumulProbDist = cumulProbDist;
  }

  // Helper
  private Pair<Long, Integer> retrieveTableSizeAndBlockNumber(Map<String, Object> metaData) {
    DbmsQueryResult tableSizeResult =
        (DbmsQueryResult) metaData.get(PercentilesAndCountNode.class.getSimpleName());
    tableSizeResult.rewind();
    tableSizeResult.next();
    long tableSize = tableSizeResult.getLong(PercentilesAndCountNode.TOTAL_COUNT_ALIAS_NAME);
    totalNumberOfblocks = (int) Math.ceil(tableSize / (float) blockSize);
    return Pair.of(tableSize, totalNumberOfblocks);
  }

  @Override
  public AbstractRelation getScramblingSource(
      String originalSchema, String originalTable, Map<String, Object> metaData) {
    if (primaryColumnName.isPresent()) {
      @SuppressWarnings("unchecked")
      Pair<String, String> fullTableName =
          (Pair<String, String>) metaData.get(LargeGroupListNode.class.getSimpleName());
      String largeGroupListSchemaName = fullTableName.getLeft();
      String largeGroupListTableName = fullTableName.getRight();

      JoinTable source =
          JoinTable.create(
              Arrays.<AbstractRelation>asList(
                  new BaseTable(originalSchema, originalTable, MAIN_TABLE_SOURCE_ALIAS_NAME),
                  new BaseTable(
                      largeGroupListSchemaName,
                      largeGroupListTableName,
                      RIGHT_TABLE_SOURCE_ALIAS_NAME)),
              Arrays.asList(JoinTable.JoinType.leftouter),
              Arrays.<UnnamedColumn>asList(
                  ColumnOp.equal(
                      new BaseColumn(MAIN_TABLE_SOURCE_ALIAS_NAME, primaryColumnName.get()),
                      new BaseColumn(
                          RIGHT_TABLE_SOURCE_ALIAS_NAME,
                          LargeGroupListNode.PRIMARY_GROUP_RENAME))));
      return source;
    } else {
      return new BaseTable(originalSchema, originalTable, MAIN_TABLE_SOURCE_ALIAS_NAME);
    }
  }

  @Override
  public String getMainTableAlias() {
    return MAIN_TABLE_SOURCE_ALIAS_NAME;
  }

  @Override
  public UnnamedColumn getBlockExprForTier(int tier, Map<String, Object> metaData) {
    List<Double> cumulProb = getCumulativeProbabilityDistributionForTier(metaData, tier);
    List<Double> condProb = computeConditionalProbabilityDistribution(cumulProb);
    int blockCount = cumulProb.size();

    List<UnnamedColumn> blockForTierOperands = new ArrayList<>();
    for (int j = 0; j < blockCount; j++) {
      blockForTierOperands.add(
          ColumnOp.lessequal(ColumnOp.rand(), ConstantColumn.valueOf(condProb.get(j))));
      blockForTierOperands.add(ConstantColumn.valueOf(j));
    }
    UnnamedColumn blockForTierExpr;

    if (blockForTierOperands.size() <= 1) {
      blockForTierExpr = ConstantColumn.valueOf(0);
    } else {
      blockForTierExpr = ColumnOp.casewhen(blockForTierOperands);
    }

    return blockForTierExpr;
  }

  @Override
  public int getBlockCount() {
    return totalNumberOfblocks;
  }

  @Override
  public int getTierCount() {
    return 3;
  }
}

class PercentilesAndCountNode extends QueryNodeBase {

  private static final long serialVersionUID = -1745299539668490874L;

  private String schemaName;

  private String tableName;

  private String columnMetaTokenKey;

  private String partitionMetaTokenKey;

  private Optional<String> primaryColumnName;

  public static final String AVG_PREFIX = "verdictdbavg";

  public static final String STDDEV_PREFIX = "verdictdbstddev";

  public static final String TOTAL_COUNT_ALIAS_NAME = "verdictdbtotalcount";

  public PercentilesAndCountNode(
      String schemaName,
      String tableName,
      String columnMetaTokenKey,
      String partitionMetaTokenKey,
      Optional<String> primaryColumnName) {
    super(-1, null);
    this.schemaName = schemaName;
    this.tableName = tableName;
    this.columnMetaTokenKey = columnMetaTokenKey;
    this.partitionMetaTokenKey = partitionMetaTokenKey;
    this.primaryColumnName = primaryColumnName;
  }

  @Override
  public SqlConvertible createQuery(List<ExecutionInfoToken> tokens) throws VerdictDBException {
    if (tokens.size() == 0) {
      // no token information passed
      throw new VerdictDBValueException("No token is passed.");
    }

    //    try {
    //      TimeUnit.SECONDS.sleep(1);
    //    } catch (InterruptedException e) {
    //      // TODO Auto-generated catch block
    //      e.printStackTrace();
    //    }

    @SuppressWarnings("unchecked")
    List<Pair<String, String>> columnNameAndTypes =
        (List<Pair<String, String>>) tokens.get(0).getValue(columnMetaTokenKey);

    if (columnNameAndTypes == null) {
      throw new VerdictDBValueException(
          "The passed token does not have the key: " + columnMetaTokenKey);
    }

    List<String> numericColumns = getNumericColumns(columnNameAndTypes);
    String tableSourceAlias = "t";

    // compose a select list
    List<SelectItem> selectList = new ArrayList<>();
    for (String col : numericColumns) {
      if (primaryColumnName.isPresent() && col.equals(primaryColumnName.get())) {
        continue;
      }

      // mean
      SelectItem item;
      item =
          new AliasedColumn(ColumnOp.avg(new BaseColumn(tableSourceAlias, col)), AVG_PREFIX + col);
      selectList.add(item);

      // standard deviation
      item =
          new AliasedColumn(
              ColumnOp.std(new BaseColumn(tableSourceAlias, col)), STDDEV_PREFIX + col);
      selectList.add(item);
    }
    selectList.add(new AliasedColumn(ColumnOp.count(), TOTAL_COUNT_ALIAS_NAME));

    selectQuery =
        SelectQuery.create(selectList, new BaseTable(schemaName, tableName, tableSourceAlias));
    return selectQuery;
  }

  private List<String> getNumericColumns(List<Pair<String, String>> columnNameAndTypes) {
    List<String> numericColumns = new ArrayList<>();
    for (Pair<String, String> nameAndType : columnNameAndTypes) {
      String name = nameAndType.getLeft();
      String type = nameAndType.getRight();
      if (DataTypeConverter.isNumeric(type)) {
        numericColumns.add(name);
      }
    }
    return numericColumns;
  }

  @Override
  public ExecutionInfoToken createToken(DbmsQueryResult result) {
    ExecutionInfoToken token = new ExecutionInfoToken();
    token.setKeyValue(this.getClass().getSimpleName(), result);
    return token;
  }
}

/**
 * select count(*) from originalSchema.originalTable where some-outlier-predicates
 *
 * @author Yongjoo Park
 */
class OutlierProportionNode extends QueryNodeBase {

  private static final long serialVersionUID = 3650001574444658985L;

  private String schemaName;

  private String tableName;

  public static String OUTLIER_SIZE_ALIAS = "verdictdbOutlierProportion";

  public OutlierProportionNode(String schemaName, String tableName) {
    super(-1, null);
    this.schemaName = schemaName;
    this.tableName = tableName;
  }

  @Override
  public SqlConvertible createQuery(List<ExecutionInfoToken> tokens) throws VerdictDBException {
    //    System.out.println(tokens);

    DbmsQueryResult percentileAndCountResult =
        (DbmsQueryResult) tokens.get(0).getValue(PercentilesAndCountNode.class.getSimpleName());
    String tableSourceAliasName = FastConvergeScramblingMethod.MAIN_TABLE_SOURCE_ALIAS_NAME;
    boolean pringInfoLog = true;
    UnnamedColumn outlierPrediacte =
        FastConvergeScramblingMethod.createOutlierTuplePredicate(
            percentileAndCountResult, tableSourceAliasName, pringInfoLog);

    selectQuery =
        SelectQuery.create(
            new AliasedColumn(ColumnOp.count(), OUTLIER_SIZE_ALIAS),
            new BaseTable(schemaName, tableName, tableSourceAliasName));
    selectQuery.addFilterByAnd(outlierPrediacte);

    return selectQuery;
  }

  @Override
  public ExecutionInfoToken createToken(DbmsQueryResult result) {
    ExecutionInfoToken token = new ExecutionInfoToken();
    token.setKeyValue(this.getClass().getSimpleName(), result);
    return token;
  }
}

class LargeGroupListNode extends CreateTableAsSelectNode {

  private static final long serialVersionUID = -2889642011123433574L;

  // p0 is the sampling ratio used for obtaining the large group list.
  // How to tweak this value?
  //
  // Our goal is to identify the groups that will appear anyway when sampled by simple random
  // sampling. Suppose a block size is 10, and there are 100 tuples. This means the sampling ratio
  // for a block is 0.01 (or 1%). We think if there are more than N tuples for a certain group,
  // where (1-0.01)^N <= 0.01, then the group is highly likely to appear in the first block even
  // when simple random sampled.
  //     Formally, let pi = (block size) / (table size). Let N be the number of the tuples in a
  // group. If (1 - pi)^N <= 0.01  =>  N <= log(0.01) / log(1 - pi).
  //
  //     Then, what is the good p0 for estimting N? A good rule of thumb would be to use a
  // single-block-many tuples, i.e., (block size). Then, p0 = (block size) / (table size).
  private double p0 = 0.001;

  private String schemaName;

  private String tableName;

  private String primaryColumnName;

  private long blockSize;

  public static final String PRIMARY_GROUP_RENAME = "verdictdbrenameprimarygroup";

  public static final String LARGE_GROUP_SIZE_COLUMN_ALIAS = "groupSize";

  public LargeGroupListNode(
      IdCreator idCreator,
      String schemaName,
      String tableName,
      String primaryColumnName,
      long blockSize) {
    super(idCreator, null);
    this.schemaName = schemaName;
    this.tableName = tableName;
    this.primaryColumnName = primaryColumnName;
    this.blockSize = blockSize;
  }

  /**
   * create table some-temp-table-name as select primaryGroup, count(*) * (1/p0) from
   * schemaName.tableName where rand() < p0 group by primaryGroup;
   *
   * @throws VerdictDBException
   */
  @Override
  public SqlConvertible createQuery(List<ExecutionInfoToken> tokens) throws VerdictDBException {
    String tableSourceAlias = "t";

    // search for the token that contains the table size.
    String countNodeKey = PercentilesAndCountNode.class.getSimpleName();
    long tableSize = -1;
    for (ExecutionInfoToken token : tokens) {
      if (token.containsKey(countNodeKey)) {
        DbmsQueryResult tableSizeResult = (DbmsQueryResult) token.getValue(countNodeKey);
        tableSizeResult.rewind();
        tableSizeResult.next();
        tableSize = tableSizeResult.getLong(PercentilesAndCountNode.TOTAL_COUNT_ALIAS_NAME);
        break;
      }
    }

    // set the value of p0
    if (tableSize == 0) {
      p0 = 1.0;
    } else if (tableSize != -1) {
      p0 = Math.min(1.0, blockSize / (double) tableSize);
    }

    // select
    List<SelectItem> selectList = new ArrayList<>();
    selectList.add(
        new AliasedColumn(
            new BaseColumn(tableSourceAlias, primaryColumnName), PRIMARY_GROUP_RENAME));
    selectList.add(
        new AliasedColumn(
            ColumnOp.multiply(
                ColumnOp.count(),
                ColumnOp.divide(ConstantColumn.valueOf(1.0), ConstantColumn.valueOf(p0))),
            LARGE_GROUP_SIZE_COLUMN_ALIAS));

    // from
    SelectQuery selectQuery =
        SelectQuery.create(selectList, new BaseTable(schemaName, tableName, tableSourceAlias));

    // where
    selectQuery.addFilterByAnd(ColumnOp.less(ColumnOp.rand(), ConstantColumn.valueOf(p0)));

    // group by
    selectQuery.addGroupby(new AliasReference(primaryColumnName));

    this.selectQuery = selectQuery;
    return super.createQuery(tokens);
  }

  @Override
  public ExecutionInfoToken createToken(DbmsQueryResult result) {
    ExecutionInfoToken token = super.createToken(result);
    Pair<String, String> fullTableName =
        Pair.of((String) token.getValue("schemaName"), (String) token.getValue("tableName"));

    // set duplicate information for convenience
    token.setKeyValue(this.getClass().getSimpleName(), fullTableName);
    return token;
  }
}

class LargeGroupSizeNode extends QueryNodeWithPlaceHolders {

  private static final long serialVersionUID = -7863166573727173728L;

  private String primaryColumnName;
  
  // When this node subscribes to the downstream nodes, this information must be used.
  private SubscriptionTicket subscriptionTicket;

  public static final String LARGE_GROUP_SIZE_SUM_ALIAS = "largeGroupSizeSum";

  public LargeGroupSizeNode(String primaryColumnName) {
    super(-1, null);
    this.primaryColumnName = primaryColumnName;
  
    // create a selectQuery for this node
    String tableSourceAlias = "t";
    String aliasName = LARGE_GROUP_SIZE_SUM_ALIAS;
    String groupSizeAlias = LargeGroupListNode.LARGE_GROUP_SIZE_COLUMN_ALIAS;
    
    Pair<BaseTable, SubscriptionTicket> placeholder = createPlaceHolderTable(tableSourceAlias);
    BaseTable baseTable = placeholder.getLeft();
    selectQuery =
        SelectQuery.create(
            new AliasedColumn(
                ColumnOp.sum(new BaseColumn(tableSourceAlias, groupSizeAlias)), aliasName),
            baseTable);
    subscriptionTicket = placeholder.getRight();
  }
  
  public SubscriptionTicket getSubscriptionTicket() {
    return subscriptionTicket;
  }

  /**
   * select count(*) as totalLargeGroupSize from verdicttemptable;
   *
   * @param tokens
   * @return
   * @throws VerdictDBException
   */
  @Override
  public SqlConvertible createQuery(List<ExecutionInfoToken> tokens) throws VerdictDBException {
  
//    // Note: this node already has been subscribed; thus, we don't need an explicit subscription.
//    Pair<BaseTable, SubscriptionTicket> placeholder = createPlaceHolderTable(tableSourceAlias);
//    BaseTable baseTable = placeholder.getLeft();
//    selectQuery =
//        SelectQuery.create(
//            new AliasedColumn(
//                ColumnOp.sum(new BaseColumn(tableSourceAlias, groupSizeAlias)), aliasName),
//            baseTable);
//    subscriptionTicket = placeholder.getRight();

    super.createQuery(tokens);    // placeholder replacements performed here
    return selectQuery;
  }

  @Override
  public ExecutionInfoToken createToken(DbmsQueryResult result) {
    ExecutionInfoToken token = new ExecutionInfoToken();
    token.setKeyValue(this.getClass().getSimpleName(), result);
    return token;
  }
}

/**
 * Retrieves the following information.
 *
 * <p>1. Top and bottom 0.1% percentile values of every numeric column. 2. Total count
 *
 * @author Yongjoo Park
 */
class FastConvergeStatisticsQueryGenerator implements StatiticsQueryGenerator {

  @Override
  public SelectQuery create(
      String schemaName,
      String tableName,
      List<Pair<String, String>> columnNamesAndTypes,
      List<String> partitionColumnNames) {

    // TODO Auto-generated method stub
    return null;
  }
}
