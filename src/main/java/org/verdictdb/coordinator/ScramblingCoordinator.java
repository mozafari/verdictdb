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

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.verdictdb.commons.VerdictDBLogger;
import org.verdictdb.connection.CachedDbmsConnection;
import org.verdictdb.connection.ConcurrentJdbcConnection;
import org.verdictdb.connection.DbmsConnection;
import org.verdictdb.core.execplan.ExecutablePlanRunner;
import org.verdictdb.core.scrambling.FastConvergeScramblingMethod;
import org.verdictdb.core.scrambling.ScrambleMeta;
import org.verdictdb.core.scrambling.ScramblingMethod;
import org.verdictdb.core.scrambling.ScramblingPlan;
import org.verdictdb.core.scrambling.UniformScramblingMethod;
import org.verdictdb.core.sqlobject.CreateSchemaQuery;
import org.verdictdb.core.sqlobject.CreateScrambleQuery;
import org.verdictdb.exception.VerdictDBException;
import org.verdictdb.exception.VerdictDBValueException;
import org.verdictdb.sqlwriter.QueryToSql;

import com.google.common.base.Optional;

public class ScramblingCoordinator {

  private final Set<String> scramblingMethods =
      new HashSet<>(Arrays.asList("uniform", "fastconverge"));

  // default options
  private final Map<String, String> options =
      new HashMap<String, String>() {
        private static final long serialVersionUID = -4491518418086939738L;

        {
          put("tierColumnName", "verdictdbtier");
          put("blockColumnName", "verdictdbblock");
          put("scrambleTableSuffix", "_scrambled");
          put("scrambleTableBlockSize", "1e6");
          put("createIfNotExists", "false");
        }
      };

  Optional<String> scrambleSchema;

  DbmsConnection conn;

  Optional<String> scratchpadSchema;

  private VerdictDBLogger log = VerdictDBLogger.getLogger(this.getClass());

  public ScramblingCoordinator(DbmsConnection conn) {
    this(conn, null);
  }

  public ScramblingCoordinator(DbmsConnection conn, String scrambleSchema) {
    this(conn, scrambleSchema, scrambleSchema); // uses the same schema
  }

  public ScramblingCoordinator(
      DbmsConnection conn, String scrambleSchema, String scratchpadSchema) {
    this(conn, scrambleSchema, scratchpadSchema, null);
  }

  public ScramblingCoordinator(
      DbmsConnection conn, String scrambleSchema, String scratchpadSchema, Long blockSize) {
    this.conn = conn;
    this.scratchpadSchema = Optional.fromNullable(scratchpadSchema);
    this.scrambleSchema = Optional.fromNullable(scrambleSchema);
    if (blockSize != null) {
      options.put("scrambleTableBlockSize", String.valueOf(blockSize));
    }
  }

  public ScrambleMeta scramble(String originalSchema, String originalTable)
      throws VerdictDBException {
    String newSchema;
    if (scrambleSchema.isPresent()) {
      newSchema = scrambleSchema.get();
    } else {
      newSchema = originalSchema;
    }
    String newTable = originalTable + options.get("scrambleTableSuffix");
    ScrambleMeta meta = scramble(originalSchema, originalTable, newSchema, newTable);
    return meta;
  }

  public ScrambleMeta scramble(
      String originalSchema, String originalTable, String newSchema, String newTable)
      throws VerdictDBException {

    String methodName = "uniform";
    String primaryColumn = null;
    ScrambleMeta meta =
        scramble(originalSchema, originalTable, newSchema, newTable, methodName, primaryColumn);
    return meta;
  }

  public ScrambleMeta scramble(
      String originalSchema,
      String originalTable,
      String newSchema,
      String newTable,
      String methodName)
      throws VerdictDBException {

    String primaryColumn = null;
    ScrambleMeta meta =
        scramble(originalSchema, originalTable, newSchema, newTable, methodName, primaryColumn);
    return meta;
  }

  public ScrambleMeta scramble(
      String originalSchema,
      String originalTable,
      String newSchema,
      String newTable,
      String methodName,
      String primaryColumn)
      throws VerdictDBException {

    // copied options
    Map<String, String> customOptions = new HashMap<>(options);

    ScrambleMeta meta =
        scramble(
            originalSchema,
            originalTable,
            newSchema,
            newTable,
            methodName,
            primaryColumn,
            customOptions);
    return meta;
  }

  public ScrambleMeta scramble(CreateScrambleQuery query) throws VerdictDBException {

    String originalSchema = query.getOriginalSchema();
    String originalTable = query.getOriginalTable();
    String newSchema = query.getNewSchema();
    String newTable = query.getNewTable();
    String methodName = query.getMethod();
    String primaryColumn = null;
    Map<String, String> customOptions = new HashMap<>(options);

    ScrambleMeta meta =
        scramble(
            originalSchema,
            originalTable,
            newSchema,
            newTable,
            methodName,
            primaryColumn,
            customOptions);

    return meta;

    //    // sanity check
    //    if (!scramblingMethods.contains(methodName.toLowerCase())) {
    //      throw new VerdictDBValueException("Not supported scrambling method: " + methodName);
    //    }
    //
    //    // overwrite options with custom options.
    //    Map<String, String> effectiveOptions = new HashMap<String, String>();
    //    for (Entry<String, String> o : options.entrySet()) {
    //      effectiveOptions.put(o.getKey(), o.getValue());
    //    }
    //    for (Entry<String, String> o : customOptions.entrySet()) {
    //      effectiveOptions.put(o.getKey(), o.getValue());
    //    }
    //
    //    if (query.isIfNotExists()) {
    //      effectiveOptions.put("createIfNotExists", "true");
    //    }
    //
    //    // determine scrambling method
    //    long blockSize =
    // Double.valueOf(effectiveOptions.get("scrambleTableBlockSize")).longValue();
    //    ScramblingMethod scramblingMethod;
    //    if (methodName.equalsIgnoreCase("uniform")) {
    //      scramblingMethod = new UniformScramblingMethod(blockSize);
    //    } else if (methodName.equalsIgnoreCase("FastConverge") && primaryColumn == null) {
    //      scramblingMethod = new FastConvergeScramblingMethod(blockSize, scratchpadSchema.get());
    //    } else if (methodName.equalsIgnoreCase("FastConverge") && primaryColumn != null) {
    //      scramblingMethod =
    //          new FastConvergeScramblingMethod(blockSize, scratchpadSchema.get(), primaryColumn);
    //    } else {
    //      throw new VerdictDBValueException("Invalid scrambling method: " + methodName);
    //    }
    //
    //    // perform scrambling
    //    ScramblingPlan plan =
    //        ScramblingPlan.create(
    //            newSchema, newTable, originalSchema, originalTable, scramblingMethod,
    // effectiveOptions);
    //    ExecutablePlanRunner.runTillEnd(conn, plan);
    //
    //    // compose scramble meta
    //    String blockColumn = effectiveOptions.get("blockColumnName");
    //    int blockCount = scramblingMethod.getBlockCount();
    //    String tierColumn = effectiveOptions.get("tierColumnName");
    //    int tierCount = scramblingMethod.getTierCount();
    //
    //    Map<Integer, List<Double>> cumulativeDistribution = new HashMap<>();
    //    for (int i = 0; i < tierCount; i++) {
    //      List<Double> dist =
    // scramblingMethod.getStoredCumulativeProbabilityDistributionForTier(i);
    //      cumulativeDistribution.put(i, dist);
    //    }
    //
    //    ScrambleMeta meta =
    //        new ScrambleMeta(
    //            newSchema,
    //            newTable,
    //            originalSchema,
    //            originalTable,
    //            blockColumn,
    //            blockCount,
    //            tierColumn,
    //            tierCount,
    //            cumulativeDistribution,
    //            methodName);

    //    return meta;
  }

  public ScrambleMeta scramble(
      String originalSchema,
      String originalTable,
      String newSchema,
      String newTable,
      String methodName,
      String primaryColumn,
      Map<String, String> customOptions)
      throws VerdictDBException {

    // sanity check
    if (!scramblingMethods.contains(methodName.toLowerCase())) {
      throw new VerdictDBValueException("Not supported scrambling method: " + methodName);
    }

    // create a schema if not exists
    if (!conn.getSchemas().contains(newSchema)) {
      CreateSchemaQuery createSchemaQuery = new CreateSchemaQuery(newSchema);
      createSchemaQuery.setIfNotExists(true);
      String sql = QueryToSql.convert(conn.getSyntax(), createSchemaQuery);
      conn.execute(sql);
    }

    // overwrite options with custom options.
    Map<String, String> effectiveOptions = new HashMap<String, String>();
    for (Entry<String, String> o : options.entrySet()) {
      effectiveOptions.put(o.getKey(), o.getValue());
    }
    for (Entry<String, String> o : customOptions.entrySet()) {
      effectiveOptions.put(o.getKey(), o.getValue());
    }

    // determine scrambling method
    long blockSize = Double.valueOf(effectiveOptions.get("scrambleTableBlockSize")).longValue();
    ScramblingMethod scramblingMethod;
    if (methodName.equalsIgnoreCase("uniform")) {
      scramblingMethod = new UniformScramblingMethod(blockSize);
    } else if (methodName.equalsIgnoreCase("FastConverge") && primaryColumn == null) {
      scramblingMethod = new FastConvergeScramblingMethod(blockSize, scratchpadSchema.get());
    } else if (methodName.equalsIgnoreCase("FastConverge") && primaryColumn != null) {
      scramblingMethod =
          new FastConvergeScramblingMethod(blockSize, scratchpadSchema.get(), primaryColumn);
    } else {
      throw new VerdictDBValueException("Invalid scrambling method: " + methodName);
    }

    // perform scrambling
    log.info(
        String.format(
            "Starts to create a new scramble %s.%s from %s.%s",
            newSchema, newTable, originalSchema, originalTable));
    ScramblingPlan plan =
        ScramblingPlan.create(
            newSchema, newTable, originalSchema, originalTable, scramblingMethod, effectiveOptions);
    ExecutablePlanRunner.runTillEnd(conn, plan);
    log.info(String.format("Finished creating %s.%s", newSchema, newTable));

    // Reinitiate Connections after table creation is done
    if (conn instanceof ConcurrentJdbcConnection) {
      ((ConcurrentJdbcConnection) conn).reinitiateConnection();
    } else if (conn instanceof CachedDbmsConnection
        && ((CachedDbmsConnection) conn).getOriginalConnection() instanceof ConcurrentJdbcConnection) {
      ((ConcurrentJdbcConnection) ((CachedDbmsConnection) conn).getOriginalConnection()).reinitiateConnection();
    }

    // compose scramble meta
    String blockColumn = effectiveOptions.get("blockColumnName");
    int blockCount = scramblingMethod.getBlockCount();
    String tierColumn = effectiveOptions.get("tierColumnName");
    int tierCount = scramblingMethod.getTierCount();

    Map<Integer, List<Double>> cumulativeDistribution = new HashMap<>();
    for (int i = 0; i < tierCount; i++) {
      List<Double> dist = scramblingMethod.getStoredCumulativeProbabilityDistributionForTier(i);
      cumulativeDistribution.put(i, dist);
    }

    ScrambleMeta meta =
        new ScrambleMeta(
            newSchema,
            newTable,
            originalSchema,
            originalTable,
            blockColumn,
            blockCount,
            tierColumn,
            tierCount,
            cumulativeDistribution,
            methodName);

    return meta;
  }
}
