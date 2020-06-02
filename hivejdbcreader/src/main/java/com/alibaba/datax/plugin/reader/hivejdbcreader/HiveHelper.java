package com.alibaba.datax.plugin.reader.hivejdbcreader;

import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.alibaba.datax.common.util.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by chengmo on 2018/4/16.
 */
public class HiveHelper implements AutoCloseable {

    private static final Logger LOG = LoggerFactory
            .getLogger(HiveHelper.class);
    private static final String DRIVER = "org.apache.hive.jdbc.HiveDriver";

    protected Connection conn;
    private HiveConfig config;

    public HiveHelper(Configuration taskConfig) throws IOException {
        this.config = new HiveConfig(taskConfig);
        this.connect();
    }

    /**
     * Create table with script if it is not exists.
     *
     * @param database
     * @param tableCreateScript
     * @return
     */
    public boolean createTableIfNotExists(String database, String tableCreateScript) {
        List<String> sqlList = new ArrayList<>();
        sqlList.add("CREATE DATABASE IF NOT EXISTS " + database);
        sqlList.add("USE " + database);
        sqlList.add(tableCreateScript);
        boolean success = this.execute(sqlList);
        if (success) {
            LOG.info("Success to create table if not exists.");
        }
        return success;
    }

    /**
     * @param sqlList
     * @return
     */
    public boolean execute(List<String> sqlList) {
        boolean success = true;
        String tempSql = "";
        try {
            Statement stmt = this.conn.createStatement();
            for (String sql : sqlList) {
                tempSql = sql;
                stmt.execute(sql);
            }
        } catch (SQLException e) {
            success = false;
            LOG.error("Execute error with sql:\n{0}", e, tempSql);
        }
        return success;
    }

    /**
     * @param sql
     * @return
     */
    public boolean execute(String sql) {
        boolean success = true;
        try {
            Statement stmt = this.conn.createStatement();
            stmt.execute(sql);
            LOG.info("SQL>>{0}", sql);
        } catch (SQLException e) {
            success = false;
            LOG.error("Execute error with sql:\n{0}", e, sql);
        }
        return success;
    }

    /**
     * 批量记载数据，防止数据一次性加载，造成OOM
     *
     * @param sql
     * @param fetchSize
     * @return
     */
    public ResultSet executeQuery(String sql, int fetchSize){
        ResultSet resultSet = null;
        try {
            Statement statement = this.conn.createStatement();
            statement.setFetchSize(fetchSize);
            resultSet =  statement.executeQuery(sql);
        } catch (SQLException e){
            LOG.error("Execute query error with sql:"+sql+"||exception:"+e.getMessage(), e);
        }
        return resultSet;

    }

    /**
     * @param sql
     * @return
     */
    public List<Map<String, Object>> executeQuery(String sql) {
        List<Map<String, Object>> resultList = new ArrayList<>();
        try {
            Statement stmt = this.conn.createStatement();
            ResultSet rs = stmt.executeQuery(sql);
            ResultSetMetaData rsmd = rs.getMetaData();
            int count = rsmd.getColumnCount();
            while (rs.next()) {
                Map<String, Object> row = new HashMap<>();
                for (int i = 0; i < count; i++) {
                    row.put(rsmd.getColumnName(i + 1), rs.getObject(i + 1));
                }
                resultList.add(row);
            }
        } catch (SQLException e) {
            LOG.error("Execute query error with sql:\n{0}", e, sql);
        }
        return resultList;
    }

    public void close() {
        try {
            if (conn == null || conn.isClosed()) {
                return;
            }
            conn.close();
        } catch (SQLException e) {
            LOG.error(e.getMessage(),e);
        }
    }

    ///////////////////////
    // private functions
    ///////////////////////
    private void connect() {
        try {
            if (conn != null && !conn.isClosed()) {
                return;
            }
            Class.forName(DRIVER);


            conn = DriverManager.getConnection(config.getUrl(),
                    config.getUserName(), config.getPassword());
            LOG.info("Success to connect hive jdbc with url:"+config.getUrl(), config.getUrl());
        } catch (ClassNotFoundException e) {
            LOG.error("ClassNotFound",e);
            throw new RuntimeException(e);
        } catch (Exception e) {
            LOG.error("Failed to connect hive jdbc with url:"+config.getUrl(), e, config.getUrl());
            throw new RuntimeException(e);
        }
    }

    public void testConnect() {
        connect();
        try {
            conn.createStatement().executeQuery("show tables");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            try {
                conn.close();
            } catch (SQLException e) {
                // ignore
            }
        }
    }
}
