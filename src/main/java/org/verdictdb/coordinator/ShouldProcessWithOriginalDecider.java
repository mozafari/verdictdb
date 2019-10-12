/*
 *    Copyright 2019 University of Michigan
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

import java.util.List;

import com.google.common.base.Optional;

import org.verdictdb.VerdictSingleResult;
import org.verdictdb.core.querying.ola.Dimension;
import org.verdictdb.core.querying.ola.HyperTableCube;
import org.verdictdb.core.sqlobject.SelectQuery;
import org.verdictdb.core.sqlobject.ConstantColumn;
import org.verdictdb.core.scrambling.ScrambleMeta;
import org.verdictdb.core.scrambling.ScrambleMetaSet;

public class ShouldProcessWithOriginalDecider {
  private ScrambleMetaSet metaset;
  private float ratioToUseOriginalAfter = -1;

  public ShouldProcessWithOriginalDecider(SelectQuery selectQuery, ScrambleMetaSet metaset) {
    Optional<ConstantColumn> useOriginalAfter = selectQuery.getUseOriginalAfter();
    if (useOriginalAfter.isPresent()) {
      ratioToUseOriginalAfter = (float) useOriginalAfter.get().getValue();
    }

    this.metaset = metaset;
  }

  public boolean shouldRunOriginal(VerdictSingleResult result) {
    if (ratioToUseOriginalAfter == -1) {
      return false;
    }

    List<HyperTableCube> blocks = result.getMetaData().coveredCubes;

    Double percTableUsed = 0.0;
    for (HyperTableCube block : blocks) {
      percTableUsed += percTableUsedForHyperTableCube(block);
    }

    return percTableUsed >= ratioToUseOriginalAfter;
  }


  private Double percTableUsedForHyperTableCube(HyperTableCube block) {
    Double result = 1.0;
    for (Dimension d : block.getDimensions()) {
      result *= percTableUsedDim(d);
    }

    return result;
  }


  private Double percTableUsedDim(Dimension dim) {
    ScrambleMeta scrambleMeta = metaset.getSingleMeta(
      dim.getSchemaName(), dim.getTableName()
    );

    Double result = 0.0;
    int numTiers = scrambleMeta.getNumberOfTiers();
    for (int i = 0; i < numTiers; i++) {
      List<Double> tierProbDistrib 
        = scrambleMeta.getCumulativeDistributionForTier(i);

      Double probSubVal = 0.0;
      if (dim.getBegin() != 0) {
        probSubVal = tierProbDistrib.get(dim.getBegin());
      }

      result += tierProbDistrib.get(dim.getEnd()) - probSubVal;
    }

    return result;
  }
}
