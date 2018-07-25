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

import org.verdictdb.commons.AttributeValueRetrievalHelper;
import org.verdictdb.connection.DbmsQueryResultMetaData;

/**
 * Represents the result set returned from VerdictDB to the end user.
 *
 * @author Yongjoo Park
 */
public abstract class VerdictSingleResult extends AttributeValueRetrievalHelper {

  public abstract boolean isEmpty();

  public abstract DbmsQueryResultMetaData getMetaData();

  @Override
  public abstract int getColumnCount();

  @Override
  public abstract String getColumnName(int index);

  public abstract int getColumnType(int index);

  public abstract long getRowCount();

  @Override
  public abstract Object getValue(int index);

  public abstract boolean wasNull();

  public abstract boolean next();

  public abstract void rewind();
}
