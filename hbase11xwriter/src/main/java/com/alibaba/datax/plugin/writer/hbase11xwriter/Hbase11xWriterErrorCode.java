package com.alibaba.datax.plugin.writer.hbase11xwriter;

import com.alibaba.datax.common.spi.ErrorCode;

/**
 * Hbase11xWriterErrorCode
 * Created by shf on 16/3/8.
 */
public enum Hbase11xWriterErrorCode implements ErrorCode {
    REQUIRED_VALUE("Hbasewriter-00", "您缺失了必须填写的参数值."),
    ILLEGAL_VALUE("Hbasewriter-01", "您填写的参数值不合法."),
    GET_HBASE_CONNECTION_ERROR("Hbasewriter-02", "获取Hbase连接时出错."),
    GET_HBASE_TABLE_ERROR("Hbasewriter-03", "获取 Hbase  table时出错."),
    CLOSE_HBASE_CONNECTION_ERROR("Hbasewriter-04", "关闭Hbase连接时出错."),
    CLOSE_HBASE_AMIN_ERROR("Hbasewriter-05", "关闭Hbase admin时出错."),
    CLOSE_HBASE_TABLE_ERROR("Hbasewriter-06", "关闭Hbase table时时出错."),
    PUT_HBASE_ERROR("Hbasewriter-07", "写入hbase时发生IO异常."),
    DELETE_HBASE_ERROR("Hbasewriter-08", "delete hbase表时发生异常."),
    TRUNCATE_HBASE_ERROR("Hbasewriter-09", "truncate hbase表时发生异常."),
    CONSTRUCT_ROWKEY_ERROR("Hbasewriter-10", "构建rowkey时发生异常."),
    CONSTRUCT_VERSION_ERROR("Hbasewriter-11", "构建version时发生异常."),
    GET_HBASE_BUFFEREDMUTATOR_ERROR("Hbasewriter-12", "获取hbase BufferedMutator 时出错."),
    CLOSE_HBASE_BUFFEREDMUTATOR_ERROR("Hbasewriter-13", "关闭 Hbase BufferedMutator时出错."),
    KERBEROS_INIT_ERROR("Hbasewriter-14", "kerberos初始化失败"),
    HBASESITE_NOTFOUND_ERROR("Hbasewriter-15", "hbasesite配置为空"),
    HDFSSITE_NOTFOUND_ERROR("Hbasewriter-16", "hdfssite配置为空"),
    CORESITE_NOTFOUND_ERROR("Hbasewriter-17", "coresite配置为空"),
    USERKEYTABFILE_NOTFOUND_ERROR("Hbasewriter-18", "kerberosKeytabFilePath配置为空"),
    KRB5CONF_NOTFOUND_ERROR("Hbasewriter-19", "krb5conf配置为空"),
    KERBEROSPRINCIPAL_NOTFOUND_ERROR("Hbasewriter-20", "kerberosPrincipal配置为空")
    ;
    private final String code;
    private final String description;

    private Hbase11xWriterErrorCode(String code, String description) {
        this.code = code;
        this.description = description;
    }

    @Override
    public String getCode() {
        return this.code;
    }

    @Override
    public String getDescription() {
        return this.description;
    }

    @Override
    public String toString() {
        return String.format("Code:[%s], Description:[%s].", this.code,
                this.description);
    }
}
