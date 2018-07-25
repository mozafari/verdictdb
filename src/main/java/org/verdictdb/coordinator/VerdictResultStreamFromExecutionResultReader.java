package org.verdictdb.coordinator;

import org.verdictdb.connection.DbmsQueryResult;
import org.verdictdb.core.resulthandler.ExecutionResultReader;

import java.util.Iterator;

public class VerdictResultStreamFromExecutionResultReader extends VerdictResultStream {

  ExecutionResultReader reader;

  ExecutionContext execContext;

  public VerdictResultStreamFromExecutionResultReader(ExecutionResultReader reader, ExecutionContext execContext) {
    super();
    this.reader = reader;
    this.execContext = execContext;
  }

  // TODO
  public VerdictResultStreamFromExecutionResultReader create(VerdictSingleResult singleResult) {
    return null;
  }

  @Override
  public boolean hasNext() {
    return reader.hasNext();
  }

  @Override
  public VerdictSingleResult next() {
    DbmsQueryResult internalResult = reader.next();
    VerdictSingleResult result = new VerdictSingleResultFromDbmsQueryResult(internalResult);
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
