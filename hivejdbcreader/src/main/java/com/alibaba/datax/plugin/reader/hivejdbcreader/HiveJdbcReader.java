package com.alibaba.datax.plugin.reader.hivejdbcreader;

import com.alibaba.datax.common.element.*;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.*;
import java.util.*;

public class HiveJdbcReader  extends Reader {
    /**
     * Job 中的方法仅执行一次，Task 中方法会由框架启动多个 Task 线程并行执行。
     * <p/>
     * 整个 Reader 执行流程是：
     * <pre>
     * Job类init-->prepare-->split
     *
     * Task类init-->prepare-->startRead-->post-->destroy
     * Task类init-->prepare-->startRead-->post-->destroy
     *
     * Job类post-->destroy
     * </pre>
     */
    public static class Job extends Reader.Job {
        private static final Logger LOG = LoggerFactory
                .getLogger(Job.class);

        private Configuration readerOriginConfig = null;
        private HiveHelper hiveHelper = null;

        @Override
        public void init()  {

            LOG.info("init() begin...");
            this.readerOriginConfig = super.getPluginJobConf();
            this.validate();
            //hiveHelper = new HiveHelper(this.readerOriginConfig);
            LOG.info("init() ok and end...");
        }

        public void validate() {
            this.readerOriginConfig.getNecessaryValue(Key.HIVE_URL,
                    HiveJdbcReaderErrorCode.HIVE_URL_NOT_FIND_ERROR);
            this.readerOriginConfig.getNecessaryValue(Key.EXECUTE_SQL,
                    HiveJdbcReaderErrorCode.EXECUTE_SQL_NOT_FIND_ERROR);
            //check Kerberos
            Boolean haveKerberos = this.readerOriginConfig.getBool(Key.HAVE_KERBEROS, false);
            if (haveKerberos) {
                this.readerOriginConfig.getNecessaryValue(Key.KERBEROS_KEYTAB_FILE_PATH, HiveJdbcReaderErrorCode.REQUIRED_VALUE);
                this.readerOriginConfig.getNecessaryValue(Key.KERBEROS_PRINCIPAL, HiveJdbcReaderErrorCode.REQUIRED_VALUE);
            }

        }

        @Override
        public void prepare() {

        }

        @Override
        public List<Configuration> split(int adviceNumber) {
            List<Configuration> list =  new ArrayList<>(1);
            list.add(this.readerOriginConfig);
            return list;
        }



        @Override
        public void post() {

        }

        @Override
        public void destroy() {

        }

    }

    public static class Task extends Reader.Task {

        private static Logger LOG = LoggerFactory.getLogger(Reader.Task.class);
        private Configuration taskConfig;
        private String encoding;
        private HiveHelper hiveHelper = null;
        private String jdbcUrl;
        private String username;
        private String password;
        private int bufferSize;
        private String userPrincipal;
        private long batchSize;
        private String executeSql;
        private int taskGroupId = -1;
        private int taskId=-1;
        protected final byte[] EMPTY_CHAR_ARRAY = new byte[0];
        private static final boolean IS_DEBUG = LOG.isDebugEnabled();
        // 作为日志显示信息时，需要附带的通用信息。比如信息所对应的数据库连接等信息，针对哪个表做的操作
        private String basicMsg;

        @Override
        public void init()  {

            this.taskConfig = super.getPluginJobConf();

            this.batchSize = this.taskConfig.getInt(Key.BATCH_SIZE, 1000);
            this.encoding = this.taskConfig.getString(com.alibaba.datax.plugin.unstructuredstorage.reader.Key.ENCODING, "UTF-8");

            this.executeSql = this.taskConfig.getString(Key.EXECUTE_SQL);
            this.jdbcUrl = this.taskConfig.getString(Key.HIVE_URL);
            this.username = this.taskConfig.getString(Key.HIVE_USERNAME);
            this.password = this.taskConfig.getString(Key.HIVE_PASSWORD);
            this.userPrincipal = this.taskConfig.getString(Key.KERBEROS_PRINCIPAL);
            this.bufferSize = this.taskConfig.getInt(com.alibaba.datax.plugin.unstructuredstorage.reader.Key.BUFFER_SIZE,
                    com.alibaba.datax.plugin.unstructuredstorage.reader.Constant.DEFAULT_BUFFER_SIZE);

            try {
                this.hiveHelper = new HiveHelper(this.taskConfig);
            }catch (IOException e){
                throw DataXException.asDataXException(HiveJdbcReaderErrorCode.KERBEROS_INIT_ERROR, e.getMessage());
            }

            taskGroupId = super.getTaskGroupId();
            taskId = super.getTaskId();
            basicMsg = String.format("jdbcUrl:[%s]", this.jdbcUrl);

        }

        @Override
        public void destroy() {

        }

        @Override
        public void prepare() {

        }

        @Override
        public void startRead(RecordSender recordSender) {
            try {
                LOG.info("read start");
                //PerfRecord queryPerfRecord = new PerfRecord(taskGroupId,taskId, PerfRecord.PHASE.SQL_QUERY);
                ResultSet resultSet = hiveHelper.executeQuery(executeSql,bufferSize);
                ResultSetMetaData metaData = resultSet.getMetaData();
                int columnNumber = metaData.getColumnCount();
                while (resultSet.next()) {
                    Map<String, Object> row = new HashMap<>();
                    for (int i = 0; i < columnNumber; i++) {
                        row.put(metaData.getColumnName(i + 1), resultSet.getObject(i + 1));
                    }
                    Record record = buildRecord(recordSender,resultSet,metaData,columnNumber,super.getTaskPluginCollector());
                    recordSender.sendToWriter(record);
                }
                LOG.info("Finished read record by Sql: [{}\n] {}.",
                        executeSql, basicMsg);
            } catch (SQLException e){
                throw DataXException.asDataXException(HiveJdbcReaderErrorCode.HIVE_EXECUTE, e.getMessage());
            } finally {
                hiveHelper.close();
            }

        }

        protected Record buildRecord(RecordSender recordSender,ResultSet rs, ResultSetMetaData metaData, int columnNumber,
                                     TaskPluginCollector taskPluginCollector) {
            Record record = recordSender.createRecord();

            try {
                for (int i = 1; i <= columnNumber; i++) {
                    switch (metaData.getColumnType(i)) {
                        case Types.CHAR:
                        case Types.NCHAR:
                        case Types.VARCHAR:
                        case Types.LONGVARCHAR:
                        case Types.NVARCHAR:
                        case Types.LONGNVARCHAR:
                        case Types.CLOB:
                        case Types.NCLOB:
                            record.addColumn(new StringColumn(rs.getString(i)));
                            break;

                        case Types.SMALLINT:
                        case Types.TINYINT:
                        case Types.INTEGER:
                        case Types.BIGINT:
                            record.addColumn(new LongColumn(rs.getString(i)));
                            break;

                        case Types.NUMERIC:
                        case Types.DECIMAL:
                            record.addColumn(new DoubleColumn(rs.getString(i)));
                            break;

                        case Types.FLOAT:
                        case Types.REAL:
                        case Types.DOUBLE:
                            record.addColumn(new DoubleColumn(rs.getString(i)));
                            break;

                        case Types.TIME:
                            record.addColumn(new DateColumn(rs.getTime(i)));
                            break;

                        // for mysql bug, see http://bugs.mysql.com/bug.php?id=35115
                        case Types.DATE:
                            if (metaData.getColumnTypeName(i).equalsIgnoreCase("year")) {
                                record.addColumn(new LongColumn(rs.getInt(i)));
                            } else {
                                record.addColumn(new DateColumn(rs.getDate(i)));
                            }
                            break;

                        case Types.TIMESTAMP:
                            record.addColumn(new DateColumn(rs.getTimestamp(i)));
                            break;

                        case Types.BINARY:
                        case Types.VARBINARY:
                        case Types.BLOB:
                        case Types.LONGVARBINARY:
                            record.addColumn(new BytesColumn(rs.getBytes(i)));
                            break;

                        // warn: bit(1) -> Types.BIT 可使用BoolColumn
                        // warn: bit(>1) -> Types.VARBINARY 可使用BytesColumn
                        case Types.BOOLEAN:
                        case Types.BIT:
                            record.addColumn(new BoolColumn(rs.getBoolean(i)));
                            break;

                        case Types.NULL:
                            String stringData = null;
                            if(rs.getObject(i) != null) {
                                stringData = rs.getObject(i).toString();
                            }
                            record.addColumn(new StringColumn(stringData));
                            break;

                        case Types.ARRAY:
                            record.addColumn(new StringColumn(rs.getObject(i).toString()));
                            break;

                        default:
                            throw DataXException
                                    .asDataXException(
                                            HiveJdbcReaderErrorCode.UNSUPPORTED_TYPE,
                                            String.format(
                                                    "您的配置文件中的列配置信息有误. 因为DataX 不支持数据库读取这种字段类型. 字段名:[%s], 字段名称:[%s], 字段Java类型:[%s]. 请尝试使用数据库函数将其转换datax支持的类型 或者不同步该字段 .",
                                                    metaData.getColumnName(i),
                                                    metaData.getColumnType(i),
                                                    metaData.getColumnClassName(i)));
                    }
                }
            } catch (Exception e) {
                if (IS_DEBUG) {
                    LOG.debug("read data " + record.toString()
                            + " occur exception:", e);
                }
                //TODO 这里识别为脏数据靠谱吗？
                taskPluginCollector.collectDirtyRecord(record, e);
                if (e instanceof DataXException) {
                    throw (DataXException) e;
                }
            }
            return record;
        }


    }
}
