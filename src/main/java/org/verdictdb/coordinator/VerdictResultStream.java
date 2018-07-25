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

import java.util.Iterator;

public abstract class VerdictResultStream
    implements Iterable<VerdictSingleResult>, Iterator<VerdictSingleResult> {

  // TODO
  public abstract VerdictResultStream create(VerdictSingleResult singleResult);

  @Override
  public abstract boolean hasNext();

  @Override
  public abstract VerdictSingleResult next();

  @Override
  public abstract Iterator<VerdictSingleResult> iterator();

  @Override
  public abstract void remove();

  public abstract void close();
}
