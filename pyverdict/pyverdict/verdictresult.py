
from datetime import date, datetime, timedelta
import decimal
import numpy as np


class SingleResultSet:
    """Reads the data from the Java SingleResultSet

    The data retrieval roughly follows PEP 249:
    https://www.python.org/dev/peps/pep-0249/#type-objects-and-constructors

    Due to the reliance on the underlying JDBC library, we cannot always distinguish the exact
    type of the underlying data; we can only set the type according to the type indentifier as
    indicated by the JDBC library itself. For example, MySQL's library does not distinguish bit and
    boolean by passing -7 for both types while their actual meanings are different and they are
    separate types defined in java.sql.Types.

    Type conversion rule (MySQL):
    'bit'                     => boolean,   # due to MySQL driver bug ('bit' is treated as boolean)
    'tinyint'                 => int,
    'bool'                    => boolean,
    'smallint'                => int,
    'medimumInteger'          => int,
    'int'                     => int,
    'integer                  => int,
    'bigint'                  => int,
    'decimal'                 => decimal.Decimal,
    'dec'                     => decimal.Decimal,
    'real'                    => float,
    'double'                  => float,
    'doubleprecision'         => float,
    'date'      => JavaObject => datetime.date,
    'datetime'  => JavaObject => datetime.datetime,
    'timestamp' => JavaObject => datetime.datetime,
    'time'      => JavaObject => datetime.timedelta,
    'year(2)'   => JavaObject => datetime.date,
    'year(4)'   => JavaObject => datetime.date,
    'char'                    => str,
    'varchar'                 => str,
    'binary'                  => bytes,
    'varbinary'               => bytes,
    'tinyblob'                => bytes,
    'tinytext'                => str,
    'blob'                    => bytes,
    'text'                    => str,
    'mediumBlob'              => bytes,
    'medimumText'             => str,
    'longBlob'                => bytes,
    'longText'                => str,
    'enumCol'                 => str,
    'setCol'                  => str
    """

    type_to_read_in_str = set(['date', 'timestamp', 'time'])

    python_type_to_numpy_type = {
        bool: np.bool_,
        int: np.int64,
        decimal.Decimal: np.float64,
        str: np.unicode_,
        bytes: np.unicode_,
        date: np.datetime64,
        datetime: np.datetime64,
        timedelta: np.timedelta64
    }

    def __init__(self, resultset):
        (heading, column_inttypes, column_types, rows) = self._read_all(resultset)
        self._heading = heading
        self._column_inttypes = column_inttypes
        self._column_types = column_types
        self._rows = rows
        self.rowcount = len(rows)

    def rows(self):
        return self._rows

    def fetchall(self):
        return self.rows()

    def types(self):
        return self._column_types

    def typeJavaInt(self):
        return self._column_inttypes

    def to_df(self):
        """
        Converts to a numpy array.
        """
        return None

    def _read_value(self, resultset, index, col_type):
        """Reads the value in a type-sensitive way

        Time-related Java objects are read as py4j.JavaObject by default. To avoid this, we read
        them as str and convert to an appropriate python object.

        Args
            resultset: the Java result set currently in process
            index: zero-based index of the column to read
            col_type: column type in str
        """
        if col_type in SingleResultSet.type_to_read_in_str:
            value_str = resultset.getString(index)
            if value_str is None:
                return None

            if col_type == 'date':
                return datetime.strptime(value_str, "%Y-%m-%d").date()
            elif col_type == 'timestamp':
                value_str = value_str[:19]      # 19 == len('2018-12-31 00:00:01')
                return datetime.strptime(value_str, "%Y-%m-%d %H:%M:%S")
            elif col_type == 'time':
                t = datetime.strptime(value_str, "%H:%M:%S")
                return timedelta(hours=t.hour, minutes=t.minute, seconds=t.second)
            else:
                return None         # not supposed to reach here
        # if col_type == 'bit':
        #     value = resultset.getByte(index)
        #     return value        # this will return int 0 or int 1
        else:
            return resultset.getValue(index)

    def _read_all(self, resultset):
        column_count = resultset.getColumnCount()
        heading = []          # column heading
        column_inttypes = []  # column types in java.sql.Types
        column_types = []     # column type
        rows = []             # data in the table

        for i in range(column_count):
            heading.append(resultset.getColumnName(i))
            column_inttypes.append(resultset.getColumnType(i))
            column_types.append(resultset.getColumnTypeName(i))

        while (resultset.next()):
            row = []
            for i in range(column_count):
                row.append(self._read_value(resultset, i, column_types[i]))
            rows.append(row)

        return (heading, column_inttypes, column_types, rows)
