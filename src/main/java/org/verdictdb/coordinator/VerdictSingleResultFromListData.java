package org.verdictdb.coordinator;

import com.google.common.base.Optional;
import com.rits.cloning.Cloner;
import org.verdictdb.connection.DataTypeConverter;
import org.verdictdb.connection.DbmsQueryResult;
import org.verdictdb.connection.DbmsQueryResultMetaData;

import java.io.*;
import java.sql.Types;
import java.util.List;

public class VerdictSingleResultFromListData extends VerdictSingleResult {

  private Optional<List<Object>> result;

  // used to support wasnull()
  private Object lastValueRead;

  int cursor = -1;

  public VerdictSingleResultFromListData(List<Object> result) {
    super();
    if (result == null) {
      this.result = Optional.absent();
    } else {
      List<Object> copied = copyResult(result);
      this.result = Optional.of(copied);
    }
  }

  public VerdictSingleResultFromListData(List<Object> result, boolean asIs) {
    // If result contains objects that cannot be serialized (e.g., BLOB, CLOB in H2),
    // it is just copied as-is (i.e., shallow copy) as opposed to deep copy.
    super();
    if (result == null) {
      this.result = Optional.absent();
    } else {
      if (asIs) {
        this.result = Optional.of(result);
      } else {
        List<Object> copied = copyResult(result);
        this.result = Optional.of(copied);
      }
    }
  }

  public static VerdictSingleResultFromListData empty() {
    return new VerdictSingleResultFromListData(null);
  }

  public boolean isEmpty() {
    return !result.isPresent();
  }


  private List<Object> copyResult(List<Object> result) {
    List<Object> copied = new Cloner().deepClone(result);
    return copied;
  }

  public DbmsQueryResultMetaData getMetaData() {
    return null;
  }

  @Override
  public int getColumnCount() {
    if (result.isPresent() == false) {
      return 0;
    } else {
      Object o = result.get().get(0);
      if (o instanceof List) {
        return ((List) o).size();
      }
      else return 1;
    }
  }

  @Override
  public String getColumnName(int index) {
    if (result.isPresent() == false || result.get().isEmpty()) {
      throw new RuntimeException("An empty result is accessed.");
    } else {
      Object o = result.get().get(0);
      if (o instanceof String) return "columnLabels";
      else if (o instanceof Integer) return "columnTypes";
      else if (o instanceof List) return "values";
      else throw new RuntimeException("Unexpected columns");
    }
  }

  public int getColumnType(int index) {
    if (result.isPresent() == false || result.get().isEmpty()) {
      throw new RuntimeException("An empty result is accessed.");
    } else {
      Object o = result.get().get(0);
      if (o instanceof String) return DataTypeConverter.typeInt("varchar");
      else if (o instanceof Integer) return DataTypeConverter.typeInt("int");
      else if (o instanceof List) return Types.JAVA_OBJECT;
      else throw new RuntimeException("Unexpected columns");
    }
  }

  public long getRowCount() {
    if (result.isPresent() == false) {
      return 0;
    } else {
      return result.get().size();
    }
  }

  @Override
  public Object getValue(int index) {
    if (result.isPresent() == false) {
      throw new RuntimeException("An empty result is accessed.");
    } else {
      Object o = result.get().get(cursor);
      if (o instanceof List) {
        return ((List) o).get(index);
      }
      else {
        if (index!=0) {
          throw new RuntimeException("Index is out of boundary.");
        }
        return o;
      }
    }
  }

  public boolean wasNull() {
    return lastValueRead == null;
  }

  public boolean next() {
    if (result.isPresent() == false) {
      return false;
    } else {
      if (cursor < getRowCount() - 1) {
        cursor++;
        return true;
      } else {
        return false;
      }
    }
  }

  public boolean hasNext() {
    if (result.isPresent() == false) {
      return false;
    } else {
      return cursor < getRowCount() - 1;
    }
  }

  public void rewind() {
    cursor = -1;
  }
}
