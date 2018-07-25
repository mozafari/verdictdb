package org.verdictdb.coordinator;

import org.verdictdb.connection.DbmsQueryResult;
import org.verdictdb.core.resulthandler.ExecutionResultReader;

import java.util.Iterator;

public class VerdictResultStreamFromSingleResult extends VerdictResultStream {

  VerdictSingleResult result;

  boolean nextHaveCalled = false;

  public VerdictResultStreamFromSingleResult(VerdictSingleResult result) {
    super();
    this.result = result;
  }

  public VerdictResultStreamFromSingleResult create(VerdictSingleResult singleResult) {
    return new VerdictResultStreamFromSingleResult(singleResult);
  }

  @Override
  public boolean hasNext() {
    return !nextHaveCalled;
  }

  @Override
  public VerdictSingleResult next() {
    nextHaveCalled = true;
    return result;
  }

  @Override
  public Iterator<VerdictSingleResult> iterator() {
    return this;
  }

  @Override
  public void remove() {

  }

  public void close() {

  }

}
