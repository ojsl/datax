package com.alibaba.datax.plugin.reader.hivejdbcreader;

import com.alibaba.datax.common.spi.ErrorCode;

public enum HiveJdbcReaderErrorCode implements ErrorCode {
    BAD_CONFIG_VALUE("HiveJdbcReader-00", "您配置的值不合法."),
    ILLEGAL_VALUE("HiveJdbcReader-01", "值错误"),
    CONFIG_INVALID_EXCEPTION("HiveJdbcReader-02", "参数配置错误"),
    REQUIRED_VALUE("HiveJdbcReader-03", "您缺失了必须填写的参数值."),
    HIVE_URL_NOT_FIND_ERROR("HiveJdbcReader-04","您未配置HiveUrl值"),
    EXECUTE_SQL_NOT_FIND_ERROR("HiveJdbcReader-05","您未配置EXECUTE_SQL值"),
    NO_INDEX_VALUE("HiveJdbcReader-06","没有 Index" ),
    MIXED_INDEX_VALUE("HiveJdbcReader-07","index 和 value 混合" ),
    READ_FILE_ERROR("HiveJdbcReader-08", "读取文件出错"),
    FILE_TYPE_ERROR("HiveJdbcReader-09", "文件类型配置错误"),
    KERBEROS_LOGIN_ERROR("HiveJdbcReader-10", "KERBEROS认证失败"),
    UNSUPPORTED_TYPE("HiveJdbcReader-11", "不支持的数据库类型. 请注意查看 DataX 已经支持的数据库类型以及数据库版本."),
    HIVE_EXECUTE("HiveJdbcReader-12", "HiveSql执行异常"),
    KERBEROS_INIT_ERROR("HiveJdbcReader-13", "kerberos初始化失败，触发了IO异常。");

    private final String code;
    private final String description;

    private HiveJdbcReaderErrorCode(String code, String description) {
        this.code = code;
        this.description = description;
    }

    @Override
    public String getCode() {
        return null;
    }

    @Override
    public String getDescription() {
        return null;
    }

    @Override
    public String toString() {
        return String.format("Code:[%s], Description:[%s]. ", this.code,
                this.description);
    }
}
