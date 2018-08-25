/*
 * Copyright 2017 University of Michigan
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.umich.verdict.dbms;

import com.google.common.base.Joiner;
import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.datatypes.SampleParam;
import edu.umich.verdict.datatypes.TableUniqueName;
import edu.umich.verdict.exceptions.VerdictException;
import edu.umich.verdict.util.StringManipulations;

import java.util.List;

public class DbmsMysql extends DbmsJDBC {

    public DbmsMysql(VerdictContext vc, String dbName, String host, String port, String schema, String user,
                     String password, String jdbcClassName) throws VerdictException {
        super(vc, dbName, host, port, schema, user, password, jdbcClassName);
    }

    @Override
    public void insertEntry(TableUniqueName tableName, List<Object> values) throws VerdictException {
        StringBuilder sql = new StringBuilder(1000);
        sql.append(String.format("insert into table %s select * from (select ", tableName));
        String with = "'";
        sql.append(Joiner.on(", ").join(StringManipulations.quoteString(values, with)));
        sql.append(") s");
        executeUpdate(sql.toString());
    }

    @Override
    public String getQuoteString() {
        return "`";
    }

    @Override
    protected String modOfRand(int mod) {
        return String.format("abs(rand(unix_timestamp())) %% %d", mod);
    }

    @Override
    public String modOfHash(String col, int mod) {
        return String.format("crc32(cast(%s%s%s as char)) %% %d", getQuoteString(), col, getQuoteString(), mod);
    }

    @Override
    public String modOfHash(List<String> columns, int mod) {
        String concatStr = "";
        for (int i = 0; i < columns.size(); ++i) {
            String col = columns.get(i);
            String castStr = String.format("cast(%s%s%s as char)", getQuoteString(), col, getQuoteString());
            if (i < columns.size() - 1) {
                castStr += ",";
            }
            concatStr += castStr;
        }
        return String.format("crc32(concat_ws('%s', %s)) %% %d", HASH_DELIM, concatStr, mod);
    }

    @Override
    protected String randomNumberExpression(SampleParam param) {
        String expr = "rand(unix_timestamp())";
        return expr;
    }

    @Override
    protected String randomPartitionColumn() {
        int pcount = partitionCount();
        return String.format("abs(round(rand(unix_timestamp())*%d) %% %d) AS %s", pcount, pcount, partitionColumnName());
    }

    @Override
    public Dataset<Row> getDataset() {
        // TODO Auto-generated method stub
        return null;
    }

}
