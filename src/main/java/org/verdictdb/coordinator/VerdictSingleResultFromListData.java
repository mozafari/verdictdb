package org.verdictdb.coordinator;

import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.verdictdb.VerdictSingleResult;
import org.verdictdb.commons.DataTypeConverter;
import org.verdictdb.connection.DbmsQueryResultMetaData;

import com.google.common.base.Optional;
import com.rits.cloning.Cloner;

public class VerdictSingleResultFromListData
    extends VerdictSingleResult {

  private Optional<List<List<Object>>> result;

  List<String> fieldsName = new ArrayList<>();

  // used to support wasnull()
  private Object lastValueRead;

  int cursor = -1;

  public VerdictSingleResultFromListData(){}

  public VerdictSingleResultFromListData(List<String> header, List<List<Object>> result) {
    super();
    if (result == null) {
      this.result = Optional.absent();
    } else {
      fieldsName = header;
      List<List<Object>>  copied = copyResult(result);
      this.result = Optional.of(copied);
    }
  }

  public VerdictSingleResultFromListData(List<String> header, List<List<Object>>  result, boolean asIs) {
    // If result contains objects that cannot be serialized (e.g., BLOB, CLOB in H2),
    // it is just copied as-is (i.e., shallow copy) as opposed to deep copy.
    super();
    if (result == null) {
      this.result = Optional.absent();
    } else {
      if (asIs) {
        this.result = Optional.of(result);
      } else {
        fieldsName = header;
        List<List<Object>>  copied = copyResult(result);
        this.result = Optional.of(copied);
      }
    }
  }

  public static VerdictSingleResultFromListData empty() {
    return new VerdictSingleResultFromListData(null, null);
  }

  public boolean isEmpty() {
    return !result.isPresent();
  }


  private static List<List<Object>>  copyResult(List<List<Object>>  result) {
    List<List<Object>>  copied = new Cloner().deepClone(result);
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
    if (result.isPresent() == false) {
      throw new RuntimeException("An empty result is accessed.");
    } else {
      return fieldsName.get(index);
    }
  }

  public int getColumnType(int index) {
    if (result.isPresent() == false || result.get().isEmpty()) {
      throw new RuntimeException("An empty result is accessed.");
    } else {
      Object o = (result.get().get(0)).get(index);
      if (o instanceof String) {
        return DataTypeConverter.typeInt("varchar");
      }
      else if (o instanceof Integer) {
        return DataTypeConverter.typeInt("int");
      }
      else {
        return Types.JAVA_OBJECT;
      }
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
      return result.get().get(cursor).get(index);
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

  public static VerdictSingleResultFromListData createWithSingleColumn(List<String> header, List<Object> result) {
    VerdictSingleResultFromListData singleResultFromListData = new VerdictSingleResultFromListData();
    if (result == null) {
      singleResultFromListData.result = Optional.absent();
    } else {
      List<List<Object>> rows = new ArrayList<>();
      for (Object o:result) {
        rows.add(Arrays.asList(o));
      }
      singleResultFromListData.fieldsName = header;
      List<List<Object>>  copied = copyResult(rows);
      singleResultFromListData.result = Optional.of(copied);
    }
    return singleResultFromListData;
  }
}
