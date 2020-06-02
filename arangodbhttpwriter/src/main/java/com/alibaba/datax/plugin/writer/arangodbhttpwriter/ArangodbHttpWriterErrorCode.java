package com.alibaba.datax.plugin.writer.arangodbhttpwriter;


import com.alibaba.datax.common.spi.ErrorCode;

public enum ArangodbHttpWriterErrorCode implements ErrorCode {
    BAD_CONFIG_VALUE("ArangodbHttpWrite-00", "您配置的值不合法."),
    FORMAT_ERROR("ArangodbHttpWrite-01", "参数格式异常"),
    DOCUMENT_NOT_FOUND_ERROR("ArangodbHttpWrite-02", "文档未找到"),
    VIOLATION_CONSTRAINT_ERROR("ArangodbHttpWrite-03", "当前操作违反约束"),
    COLUMN_NOT_MATCH_ERROR("ArangodbHttpWrite-04", "index与数据源列数量不一致"),
    ARANGODB_PUT_ERROR("ArangodbHttpWrite-05", "Arangodb写入异常"),
    CREATE_HCB_ERROR("ArangodbHttpWrite-05", "创建hcb实例异常"),
    URL_EMPTY_ERROR("ArangodbHttpWrite-06", "配置值url为空，请检查"),
    TYPE_EMPTY_ERROR("ArangodbHttpWrite-07", "配置值type为空，请检查"),
    COLLECTION_NAME_EMPTY_ERROR("ArangodbHttpWrite-08", "配置值collectionName为空，请检查"),
    TYPE_ERROR("ArangodbHttpWrite-08", "配置值type不符合规范，请检查"),
    COLUMN_ERROR("ArangodbHttpWrite-09", "解析column异常，请检查配置"),
    COLUMN_TYPE_error("ArangodbHttpWrite-10", "column类型配置错误，请检查"),
    PARSE_ERROR("ArangodbHttpWrite-11", "转换record类型失败，请检查reader与writer字段类型是否一致"),
    COLUMN_NOT_NULL_ERROR("ArangodbHttpWrite-12", "非空字段验证失败"),
    AUTH_FAISE_ERROR("ArangodbHttpWrite-13", "权限校验失败"),
    AUTH_PARSE_ERROR("ArangodbHttpWrite-14", "权限信息格式化失败，请检查配置"),
    AUTH_RESULT_ERROR("ArangodbHttpWrite-15", "jwt权限认证返回异常"),
    HTTP_ERROR("ArangodbHttpWrite-16", "http访问异常")
    ;

    private final String code;
    private final String description;

    private ArangodbHttpWriterErrorCode(String code, String description) {
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

