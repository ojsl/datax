package com.alibaba.datax.plugin.reader.hivejdbcreader;

public final class Key {
    /**
     * 此处声明插件用到的需要插件使用者提供的配置项
     */
    public final static String HIVE_URL = "hiveUrl";
    public final static String HIVE_USERNAME = "username";
    public static final String HIVE_PASSWORD = "password";
    public static final String HAVE_KERBEROS = "haveKerberos";
    public static final String KERBEROS_KEYTAB_FILE_PATH = "kerberosKeytabFilePath";
    public static final String KERBEROS_PRINCIPAL = "kerberosPrincipal";
    public static final String BATCH_SIZE = "batchSize";
    public static final String EXECUTE_SQL = "executeSql";
    public static final String HIVE_CONFIG = "hiveConfig";
    public static final String KRB5_CONF = "krb5conf";
    public static final String HIVE_SITE = "hivesite";
    public static final String CORE_SITE = "coresite";
}
