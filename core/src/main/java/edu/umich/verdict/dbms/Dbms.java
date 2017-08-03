package edu.umich.verdict.dbms;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.spark.sql.DataFrame;

import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.collect.Sets;

import edu.umich.verdict.VerdictConf;
import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.datatypes.SampleParam;
import edu.umich.verdict.datatypes.SampleSizeInfo;
import edu.umich.verdict.datatypes.TableUniqueName;
import edu.umich.verdict.exceptions.VerdictException;
import edu.umich.verdict.relation.ExactRelation;
import edu.umich.verdict.relation.Relation;
import edu.umich.verdict.relation.SingleRelation;
import edu.umich.verdict.relation.expr.Expr;
import edu.umich.verdict.util.VerdictLogger;

/**
 * This class is responsible for choosing a right DBMS class.
 */
public abstract class Dbms {

    protected final String dbName;

    protected Optional<String> currentSchema;

    protected VerdictContext vc;


    public VerdictContext getVc() {
        return vc;
    }

    public void setVc(VerdictContext vc) {
        this.vc = vc;
    }

    public String getDbName() {
        return dbName;
    }

    public void setCurrentSchema(Optional<String> currentSchema) {
        this.currentSchema = currentSchema;
    }

    /**
     * Copy constructor for not sharing the underlying statement.
     * @param another
     */
    public Dbms(Dbms another) {
        dbName = another.dbName;
        currentSchema = another.currentSchema;
        vc = another.vc;
    }

    protected Dbms(VerdictContext vc, String dbName) {
        this.vc = vc;
        this.dbName = dbName;
        currentSchema = Optional.absent();
    }

    public static Dbms from(VerdictContext vc, VerdictConf conf) throws VerdictException {
        Dbms dbms = Dbms.getInstance(
                vc,
                conf.getDbms(),
                conf.getHost(),
                conf.getPort(),
                conf.getDbmsSchema(),
                (conf.ignoreUserCredentials())? "" : conf.getUser(),
                        (conf.ignoreUserCredentials())? "" : conf.getPassword(),
                                conf.getDbmsClassName());

        Set<String> jdbcDbmsNames = Sets.newHashSet("mysql", "impala", "hive", "hive2", "redshift");

        if (jdbcDbmsNames.contains(conf.getDbms())) {
            VerdictLogger.info(
                    (conf.getDbmsSchema() != null) ?
                            String.format("Connected to database: %s//%s:%s/%s",
                                    conf.getDbms(), conf.getHost(), conf.getPort(), conf.getDbmsSchema())
                            : String.format("Connected to database: %s//%s:%s",
                                    conf.getDbms(), conf.getHost(), conf.getPort()));
        }

        return dbms;
    }

    protected static Dbms getInstance(VerdictContext vc,
            String dbName,
            String host,
            String port,
            String schema,
            String user,
            String password,
            String jdbcClassName) throws VerdictException {

        Dbms dbms = null;
        if (dbName.equals("mysql")) {
            dbms = new DbmsMySQL(vc, dbName, host, port, schema, user, password, jdbcClassName);
        } else if (dbName.equals("impala")) {
            dbms = new DbmsImpala(vc, dbName, host, port, schema, user, password, jdbcClassName);
        } else if (dbName.equals("hive") || dbName.equals("hive2")) {
            dbms = new DbmsHive(vc, dbName, host, port, schema, user, password, jdbcClassName);
        } else if (dbName.equals("redshift")) {
            dbms = new DbmsRedshift(vc, dbName, host, port, schema, user, password, jdbcClassName);
        } else if (dbName.equals("dummy")) {
            dbms = new DbmsDummy(vc);
        } else {
            String msg = String.format("Unsupported DBMS: %s", dbName);
            VerdictLogger.error("Dbms", msg);
            throw new VerdictException(msg);
        }

        return dbms;
    }

    public String getName() {
        return dbName;
    }

    public Optional<String> getCurrentSchema() {
        return currentSchema;
    }

    public ResultSet executeJdbcQuery(String sql) throws VerdictException {
        execute(sql);
        ResultSet rs = getResultSet();
        return rs;
    }

    public DataFrame executeSparkQuery(String sql) throws VerdictException {
        execute(sql);
        DataFrame rs = getDataFrame();
        return rs;
    }

    public abstract boolean execute(String sql) throws VerdictException;

    public abstract ResultSet getResultSet();

    public abstract DataFrame getDataFrame();

    public abstract void executeUpdate(String sql) throws VerdictException;

    public void changeDatabase(String schemaName) throws VerdictException {
        execute(String.format("use %s", schemaName));
        currentSchema = Optional.fromNullable(schemaName);
        VerdictLogger.info("Database changed to: " + schemaName);
    }

    public void createDatabase(String database) throws VerdictException {
        createCatalog(database);
    }

    public void createCatalog(String catalog) throws VerdictException {
        String sql = String.format("create database if not exists %s", catalog);
        executeUpdate(sql);
    }

    public void dropTable(TableUniqueName tableName) throws VerdictException {
        Set<String> databases = vc.getMeta().getDatabases();
        // TODO: this is buggy when the database created while a query is executued.
        // it can happen during sample creations.
        if (!databases.contains(tableName.getSchemaName())) {
            VerdictLogger.debug(this, String.format("Database, %s, does not exists. Verdict doesn't bother to run a drop table statement.", tableName.getSchemaName()));
            return;
        }

        List<String> tables = getTables(tableName.getSchemaName());
        if (!tables.contains(tableName.getTableName())) {
            VerdictLogger.debug(this, String.format("Table, %s, does not exists. Verdict doesn't bother to run a drop table statement.", tableName));
            return;
        }

        String sql = String.format("DROP TABLE IF EXISTS %s", tableName);
        VerdictLogger.debug(this, String.format("Drops table: %s", sql));
        executeUpdate(sql);
        VerdictLogger.debug(this, tableName + " has been dropped.");
    }

    public void moveTable(TableUniqueName from, TableUniqueName to) throws VerdictException {
        VerdictLogger.debug(this, String.format("Moves table %s to table %s", from, to));
        String sql = String.format("CREATE TABLE %s AS SELECT * FROM %s", to, from);
        dropTable(to);
        executeUpdate(sql);
        dropTable(from);
        VerdictLogger.debug(this, "Moving table done.");
    }

    public List<Pair<String, String>> getAllTableAndColumns(String schema) throws VerdictException {
        Set<String> databases = vc.getMeta().getDatabases();
        if (!databases.contains(schema)) {
            return Arrays.asList();
        }

        List<Pair<String, String>> tablesAndColumns = new ArrayList<Pair<String, String>>();
        List<String> tables = getTables(schema);
        for (String table : tables) {
            Map<String, String> col2type = getColumns(TableUniqueName.uname(schema, table));
            for (String column : col2type.keySet()) {
                tablesAndColumns.add(Pair.of(table, column));
            }
        }
        return tablesAndColumns;
    }

    public abstract Set<String> getDatabases() throws VerdictException;

    public abstract List<String> getTables(String schema) throws VerdictException;

    /**
     * Retrieves the mapping from column name to its type for a given table.
     * @param table
     * @return
     * @throws VerdictException
     */
    public abstract Map<String, String> getColumns(TableUniqueName table) throws VerdictException;

    public abstract void deleteEntry(TableUniqueName tableName, List<Pair<String, String>> colAndValues) throws VerdictException;

    public abstract void insertEntry(TableUniqueName tableName, List<Object> values) throws VerdictException;

    public abstract long getTableSize(TableUniqueName tableName) throws VerdictException;

    public void createMetaTablesInDMBS(
            TableUniqueName originalTableName,
            TableUniqueName sizeTableName,
            TableUniqueName nameTableName) throws VerdictException {
        VerdictLogger.debug(this, "Creates meta tables if not exist.");
        String sql = String.format("CREATE TABLE IF NOT EXISTS %s", sizeTableName)
                + " (schemaname STRING, "
                + " tablename STRING, "
                + " samplesize BIGINT, "
                + " originaltablesize BIGINT)";
        executeUpdate(sql);

        sql = String.format("CREATE TABLE IF NOT EXISTS %s", nameTableName)
                + " (originalschemaname STRING, "
                + " originaltablename STRING, "
                + " sampleschemaaname STRING, "
                + " sampletablename STRING, "
                + " sampletype STRING, "
                + " samplingratio DOUBLE, "
                + " columnnames STRING)";
        executeUpdate(sql);

        VerdictLogger.debug(this, "Meta tables created.");
    }

    public boolean doesMetaTablesExist(String schemaName) throws VerdictException {
        String metaSchema = vc.getMeta().metaCatalogForDataCatalog(schemaName);
        String metaNameTable = vc.getMeta().getMetaNameTableForOriginalSchema(schemaName).getTableName();
        String metaSizeTable = vc.getMeta().getMetaSizeTableForOriginalSchema(schemaName).getTableName();

        Set<String> tables = new HashSet<String>(getTables(metaSchema));
        if (tables.contains(metaNameTable) && tables.contains(metaSizeTable)) {
            return true;
        } else {
            return false;
        }
    }

    public Pair<Long, Long> createUniformRandomSampleTableOf(SampleParam param) throws VerdictException {
        dropTable(param.sampleTableName());
        //		justCreateUniformRandomSampleTableOf(param);
        TableUniqueName temp = createUniformRandomSampledTable(param);
        attachUniformProbabilityToTempTable(param, temp);
        dropTable(temp);
        return Pair.of(getTableSize(param.sampleTableName()), getTableSize(param.originalTable));
    }

    protected TableUniqueName createUniformRandomSampledTable(SampleParam param) throws VerdictException {
        String whereClause = String.format("__rand < %f", param.samplingRatio);
        ExactRelation sampled = SingleRelation.from(vc, param.getOriginalTable())
                .select(String.format("*, %s as __rand", randomNumberExpression(param)))
                .where(whereClause)
                .select("*, " + randomPartitionColumn());
        TableUniqueName temp = Relation.getTempTableName(vc, param.sampleTableName().getSchemaName());

        String sql = String.format("create table %s as %s", temp, sampled.toSql());
        VerdictLogger.debug(this, "The query used for creating a temporary table without sampling probabilities:");
        VerdictLogger.debugPretty(this, Relation.prettyfySql(sql), "  ");
        executeUpdate(sql);
        return temp;
    }

    protected void attachUniformProbabilityToTempTable(SampleParam param, TableUniqueName temp) throws VerdictException {
        String samplingProbCol = vc.getDbms().samplingProbabilityColumnName();
        long total_size = SingleRelation.from(vc, param.getOriginalTable()).countValue();
        long sample_size = SingleRelation.from(vc, temp).countValue();

        ExactRelation withRand = SingleRelation.from(vc, temp)
                .select("*, " + String.format("%d / %d as %s", sample_size, total_size, samplingProbCol));
        String sql = String.format("create table %s as %s", param.sampleTableName(), withRand.toSql());
        VerdictLogger.debug(this, "The query used for creating a temporary table without sampling probabilities:");
        VerdictLogger.debugPretty(this, Relation.prettyfySql(sql), "  ");
        executeUpdate(sql);
    }

    public Pair<Long, Long> createStratifiedSampleTableOf(SampleParam param) throws VerdictException {
        SampleSizeInfo info = vc.getMeta().getSampleSizeOf(new SampleParam(vc, param.originalTable, "uniform", null, new ArrayList<String>()));
        if (info == null) {
            String msg = "A uniform random must first be created before creating a stratified sample.";
            VerdictLogger.error(this, msg);
            throw new VerdictException(msg);
        }

        dropTable(param.sampleTableName());
        TableUniqueName groupSizeTemp = createGroupSizeTempTable(param);
        createStratifiedSampleFromGroupSizeTemp(param, groupSizeTemp);
        dropTable(groupSizeTemp);

        return Pair.of(getTableSize(param.sampleTableName()), getTableSize(param.originalTable));
    }

    private TableUniqueName createGroupSizeTempTable(SampleParam param) throws VerdictException {
        TableUniqueName groupSizeTemp = Relation.getTempTableName(vc, param.sampleTableName().getSchemaName());
        ExactRelation groupSize = SingleRelation.from(vc, param.originalTable)
                .groupby(param.columnNames)
                .agg("count(*) AS __group_size");
        String sql = String.format("create table %s as %s", groupSizeTemp, groupSize.toSql());
        VerdictLogger.debug(this, "The query used for the group-size temp table: ");
        VerdictLogger.debugPretty(this, Relation.prettyfySql(sql), "  ");
        executeUpdate(sql);
        return groupSizeTemp;
    }

    final protected long NULL_LONG = Long.MIN_VALUE + 1;

    final protected String NULL_STRING = "VERDICT_NULL"; 

    final protected String NULL_TIMESTAMP = "1970-01-02";      // unix timestamp starts on '1970-01-01'. We add one day just to avoid possible conlicts.

    protected void createStratifiedSampleFromGroupSizeTemp(SampleParam param, TableUniqueName groupSizeTemp) throws VerdictException {
        Map<String, String> col2types = vc.getMeta().getColumn2Types(param.originalTable);
        SampleSizeInfo info = vc.getMeta().getSampleSizeOf(new SampleParam(vc, param.originalTable, "uniform", null, new ArrayList<String>()));
        long originalTableSize = info.originalTableSize;
        long groupCount = SingleRelation.from(vc, groupSizeTemp).countValue();
        String samplingProbColName = vc.getDbms().samplingProbabilityColumnName();

        // equijoin expression that considers possible null values
        List<Pair<Expr, Expr>> joinExprs = new ArrayList<Pair<Expr, Expr>>();
        for (String col : param.getColumnNames()) {
            boolean isString = false;
            boolean isTimeStamp = false;

            if (col2types.containsKey(col)) {
                if (col2types.get(col).toLowerCase().contains("char") || col2types.get(col).toLowerCase().contains("str")) {
                    isString = true;
                } else if (col2types.get(col).toLowerCase().contains("time")) {
                    isTimeStamp = true;
                }
            }

            if (isString) {
                Expr left = Expr.from(String.format("case when s.%s is null then '%s' else s.%s end", col, NULL_STRING, col));
                Expr right = Expr.from(String.format("case when t.%s is null then '%s' else t.%s end", col, NULL_STRING, col));
                joinExprs.add(Pair.of(left, right));
            } else if (isTimeStamp) {
                Expr left = Expr.from(String.format("case when s.%s is null then '%s' else s.%s end", col, NULL_TIMESTAMP, col));
                Expr right = Expr.from(String.format("case when t.%s is null then '%s' else t.%s end", col, NULL_TIMESTAMP, col));
                joinExprs.add(Pair.of(left, right));
            } else {
                Expr left = Expr.from(String.format("case when s.%s is null then %d else s.%s end", col, NULL_LONG, col));
                Expr right = Expr.from(String.format("case when t.%s is null then %d else t.%s end", col, NULL_LONG, col));
                joinExprs.add(Pair.of(left, right));
            }
        }

        // where clause using rand function
        String whereClause = String.format("__rand < %d * %f / %d / __group_size",
                originalTableSize,
                param.getSamplingRatio(),
                groupCount);

        // aliased select list
        List<String> selectElems = new ArrayList<String>();
        for (String col : col2types.keySet()) {
            selectElems.add(String.format("s.%s", col));
        }

        // sample table
        TableUniqueName sampledNoRand = Relation.getTempTableName(vc, param.sampleTableName().getSchemaName());
        ExactRelation sampled = SingleRelation.from(vc, param.getOriginalTable())
                .select(String.format("*, %s as __rand", randomNumberExpression(param)))
                .withAlias("s")
                .join(SingleRelation.from(vc, groupSizeTemp).withAlias("t"), joinExprs)
                .where(whereClause)
                .select(Joiner.on(", ").join(selectElems) + ", __group_size");
        String sql1 = String.format("create table %s as %s", sampledNoRand, sampled.toSql());
        VerdictLogger.debug(this, "The query used for creating a stratified sample without sampling probabilities.");
        VerdictLogger.debugPretty(this, Relation.prettyfySql(sql1), "  ");
        executeUpdate(sql1);

        // attach sampling probabilities and random partition number
        ExactRelation sampledGroupSize = SingleRelation.from(vc, sampledNoRand)
                .groupby(param.columnNames)
                .agg("count(*) AS __group_size_in_sample");
        ExactRelation withRand = SingleRelation.from(vc, sampledNoRand).withAlias("s")
                .join(sampledGroupSize.withAlias("t"), joinExprs)
                .select(Joiner.on(", ").join(selectElems)
                        + String.format(", __group_size_in_sample  / __group_size as %s", samplingProbColName)
                        + ", " + randomPartitionColumn());
        String sql2 = String.format("create table %s as %s", param.sampleTableName(), withRand.toSql());
        VerdictLogger.debug(this, "The query used for creating a stratified sample with sampling probabilities.");
        VerdictLogger.debugPretty(this, Relation.prettyfySql(sql2), "  ");
        executeUpdate(sql2);

        dropTable(sampledNoRand);
    }

    //	protected abstract void justCreateStratifiedSampleTableof(SampleParam param) throws VerdictException;

    public Pair<Long, Long> createUniverseSampleTableOf(SampleParam param) throws VerdictException {
        dropTable(param.sampleTableName());
        TableUniqueName temp = createUniverseSampledTable(param);
        createUniverseSampleWithProbFromSample(param, temp);
        dropTable(temp);
        return Pair.of(getTableSize(param.sampleTableName()), getTableSize(param.originalTable));
    }

    protected TableUniqueName createUniverseSampledTable(SampleParam param) throws VerdictException {
        TableUniqueName temp = Relation.getTempTableName(vc, param.sampleTableName().getSchemaName());
        ExactRelation sampled = SingleRelation.from(vc, param.originalTable)
                .where(modOfHash(param.columnNames.get(0), 1000000) + 
                        String.format(" < %.2f", param.samplingRatio * 1000000));
        String sql = String.format("create table %s AS %s", temp, sampled.toSql());
        VerdictLogger.debug(this, "The query used for creating a universe sample without sampling probability:");
        VerdictLogger.debugPretty(this, Relation.prettyfySql(sql), "  ");
        executeUpdate(sql);
        return temp;
    }

    protected void createUniverseSampleWithProbFromSample(SampleParam param, TableUniqueName temp) throws VerdictException {
        String samplingProbCol = vc.getDbms().samplingProbabilityColumnName();
        ExactRelation sampled = SingleRelation.from(vc, temp);
        long total_size = SingleRelation.from(vc, param.originalTable).countValue();
        long sample_size = sampled.countValue();

        ExactRelation withProb = sampled.select(
                String.format("*, %d / %d AS %s", sample_size, total_size, samplingProbCol) + ", " +
                        randomPartitionColumn());
        String sql = String.format("create table %s AS %s", param.sampleTableName(), withProb.toSql());
        VerdictLogger.debug(this, "The query used for creating a universe sample with sampling probability:");
        VerdictLogger.debugPretty(this, Relation.prettyfySql(sql), "  ");
        executeUpdate(sql);
    }

    public void updateSampleNameEntryIntoDBMS(SampleParam param, TableUniqueName metaNameTableName) throws VerdictException {
        TableUniqueName tempTableName = createTempTableExlucdingNameEntry(param, metaNameTableName);
        insertSampleNameEntryIntoDBMS(param, tempTableName);
        moveTable(tempTableName, metaNameTableName);
    }

    protected TableUniqueName createTempTableExlucdingNameEntry(SampleParam param, TableUniqueName metaNameTableName) throws VerdictException {
        String metaSchema = param.sampleTableName().getSchemaName();
        TableUniqueName tempTableName = Relation.getTempTableName(vc, metaSchema);
        TableUniqueName originalTableName = param.originalTable;
        executeUpdate(String.format("CREATE TABLE %s AS SELECT * FROM %s "
                + "WHERE originalschemaname <> \"%s\" OR originaltablename <> \"%s\" OR sampletype <> \"%s\""
                + "OR samplingratio <> %s OR columnnames <> \"%s\"",
                tempTableName, metaNameTableName, originalTableName.getSchemaName(), originalTableName.getTableName(),
                param.sampleType, samplingRatioToString(param.samplingRatio), columnNameListToString(param.columnNames)));
        return tempTableName;
    }

    protected void insertSampleNameEntryIntoDBMS(SampleParam param, TableUniqueName metaNameTableName) throws VerdictException {
        TableUniqueName originalTableName = param.originalTable;
        TableUniqueName sampleTableName = param.sampleTableName();

        List<Object> values = new ArrayList<Object>();
        values.add(originalTableName.getSchemaName());
        values.add(originalTableName.getTableName());
        values.add(sampleTableName.getSchemaName());
        values.add(sampleTableName.getTableName());
        values.add(param.sampleType);
        values.add(param.samplingRatio);
        values.add(columnNameListToString(param.columnNames));

        insertEntry(metaNameTableName, values);
    }

    public void updateSampleSizeEntryIntoDBMS(SampleParam param, long sampleSize, long originalTableSize, TableUniqueName metaSizeTableName) throws VerdictException {
        TableUniqueName tempTableName = createTempTableExlucdingSizeEntry(param, metaSizeTableName);
        insertSampleSizeEntryIntoDBMS(param, sampleSize, originalTableSize, tempTableName);
        moveTable(tempTableName, metaSizeTableName);
    }

    protected TableUniqueName createTempTableExlucdingSizeEntry(SampleParam param, TableUniqueName metaSizeTableName) throws VerdictException {
        String metaSchema = param.sampleTableName().getSchemaName();
        TableUniqueName tempTableName = Relation.getTempTableName(vc, metaSchema);
        TableUniqueName sampleTableName = param.sampleTableName();
        executeUpdate(String.format("CREATE TABLE %s AS SELECT * FROM %s WHERE schemaname <> \"%s\" OR tablename <> \"%s\" ",
                tempTableName, metaSizeTableName, sampleTableName.getSchemaName(), sampleTableName.getTableName()));
        return tempTableName;
    }

    protected void insertSampleSizeEntryIntoDBMS(SampleParam param,	long sampleSize, long originalTableSize, TableUniqueName metaSizeTableName) throws VerdictException {
        TableUniqueName sampleTableName = param.sampleTableName();
        List<Object> values = new ArrayList<Object>();
        values.add(sampleTableName.getSchemaName());
        values.add(sampleTableName.getTableName());
        values.add(sampleSize);
        values.add(originalTableSize);
        insertEntry(metaSizeTableName, values);
    }

    public void deleteSampleNameEntryFromDBMS(SampleParam param, TableUniqueName metaNameTableName) throws VerdictException {
        TableUniqueName tempTable = createTempTableExlucdingNameEntry(param, metaNameTableName);
        moveTable(tempTable, metaNameTableName);
    }

    public void deleteSampleSizeEntryFromDBMS(SampleParam param, TableUniqueName metaSizeTableName) throws VerdictException {
        TableUniqueName tempTable = createTempTableExlucdingSizeEntry(param, metaSizeTableName);
        moveTable(tempTable, metaSizeTableName);
    }

    public void cacheTable(TableUniqueName tableName) {}

    /**
     * Column expression that generates a number between 0 and 99.
     * @return
     */
    protected abstract String randomPartitionColumn();

    /**
     * Column expression that generates a number between 0 and 1.
     * @return
     */
    protected abstract String randomNumberExpression(SampleParam param);

    public abstract String modOfHash(String col, int mod);

    protected abstract String modOfRand(int mod);

    protected String quote(String expr) {
        return String.format("\"%s\"", expr);
    }

    protected String columnNameListToString(List<String> columnNames) {
        return Joiner.on(",").join(columnNames);
    }

    protected String samplingRatioToString(double samplingRatio) {
        return String.format("%.4f", samplingRatio);
    }

    public String partitionColumnName() {
        return vc.getConf().subsamplingPartitionColumn();
    }

    public int partitionCount() {
        return vc.getConf().subsamplingPartitionCount();
    }

    public String samplingProbabilityColumnName() {
        return vc.getConf().subsamplingProbabilityColumn();
    }

    public boolean isJDBC() {
        return false;
    }

    public boolean isSpark() {
        return false;
    }

    public abstract void close() throws VerdictException;

    @Deprecated
    public String getQuoteString() {
        return "`";
    }

    @Deprecated
    public String varianceFunction() {
        return "VAR_SAMP";
    }

    @Deprecated
    public String stddevFunction() {
        return "STDDEV";
    }
}
