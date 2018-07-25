package org.verdictdb.coordinator;

import com.google.common.base.Optional;
import com.rits.cloning.Cloner;
import org.verdictdb.connection.DbmsQueryResult;
import org.verdictdb.connection.DbmsQueryResultMetaData;

import java.io.*;

public class VerdictSingleResultFromDbmsQueryResult extends VerdictSingleResult {

  private Optional<DbmsQueryResult> result;

  // used to support wasnull()
  private Object lastValueRead;

  public VerdictSingleResultFromDbmsQueryResult(DbmsQueryResult result) {
    super();
    if (result == null) {
      this.result = Optional.absent();
    } else {
      DbmsQueryResult copied = copyResult(result);
      copied.rewind();
      this.result = Optional.of(copied);
    }
  }

  public VerdictSingleResultFromDbmsQueryResult(DbmsQueryResult result, boolean asIs) {
    // If result contains objects that cannot be serialized (e.g., BLOB, CLOB in H2),
    // it is just copied as-is (i.e., shallow copy) as opposed to deep copy.
    super();
    if (result == null) {
      this.result = Optional.absent();
    } else {
      if (asIs) {
        this.result = Optional.of(result);
      } else {
        DbmsQueryResult copied = copyResult(result);
        copied.rewind();
        this.result = Optional.of(copied);
      }
    }
  }

  public static VerdictSingleResultFromDbmsQueryResult empty() {
    return new VerdictSingleResultFromDbmsQueryResult(null);
  }

  public boolean isEmpty() {
    return !result.isPresent();
  }


  private DbmsQueryResult copyResult(DbmsQueryResult result) {
    DbmsQueryResult copied = new Cloner().deepClone(result);
    return copied;
  }

  public DbmsQueryResultMetaData getMetaData() {
    return result.isPresent() ? result.get().getMetaData() : null;
  }

  @Override
  public int getColumnCount() {
    if (result.isPresent() == false) {
      return 0;
    } else {
      return result.get().getColumnCount();
    }
  }

  @Override
  public String getColumnName(int index) {
    if (result.isPresent() == false) {
      throw new RuntimeException("An empty result is accessed.");
    } else {
      return result.get().getColumnName(index);
    }
  }

  public int getColumnType(int index) {
    if (result.isPresent() == false) {
      throw new RuntimeException("An empty result is accessed.");
    } else {
      return result.get().getColumnType(index);
    }
  }

  public long getRowCount() {
    if (result.isPresent() == false) {
      return 0;
    } else {
      return result.get().getRowCount();
    }
  }

  @Override
  public Object getValue(int index) {
    if (result.isPresent() == false) {
      throw new RuntimeException("An empty result is accessed.");
    } else {
      Object value = result.get().getValue(index);
      lastValueRead = value;
      return value;
    }
  }

  public boolean wasNull() {
    return lastValueRead == null;
  }

  public boolean next() {
    if (result.isPresent() == false) {
      return false;
    } else {
      return result.get().next();
    }
  }

  public void rewind() {
    if (result.isPresent()) {
      result.get().rewind();
    }
  }
}
