package com.alibaba.datax.plugin.reader.hivejdbcreader;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang3.BooleanUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * Created by chengmo on 2018/4/16.
 */
public class HiveConfig {
    private String ZOOKEEPER_SERVER_PRINCIPAL_KEY = "zookeeper.server.principal";
    private String ZOOKEEPER_DEFAULT_SERVER_PRINCIPAL = "zookeeper/hadoop.hadoop.com";
    private String KERBEROS_KEYTAB_FILEPATH;
    private String KERBEROS_PRINCIPAL;
    private String KRB5_CONF;
    private String CORE_SITE;
    private String HIVE_SITE;


    private String url;
    private String userName;
    private String password;

    public HiveConfig(com.alibaba.datax.common.util.Configuration taskConfig) throws IOException {
        this.url = taskConfig.getString(Key.HIVE_URL);
        this.userName = taskConfig.getString(Key.HIVE_USERNAME);
        this.password = taskConfig.getString(Key.HIVE_PASSWORD);
        boolean security = taskConfig.getBool(Key.HAVE_KERBEROS, false);
        if (security) {
            this.KERBEROS_PRINCIPAL = taskConfig.getString(Key.KERBEROS_PRINCIPAL);
            this.KERBEROS_KEYTAB_FILEPATH = taskConfig.getString(Key.KERBEROS_KEYTAB_FILE_PATH);
            this.KRB5_CONF = taskConfig.getString(Key.KRB5_CONF);
            this.HIVE_SITE = taskConfig.getString(Key.HIVE_SITE);
            this.CORE_SITE = taskConfig.getString(Key.CORE_SITE);

            StringBuilder sb = new StringBuilder();
            sb.append("user.principal=").append(this.KERBEROS_PRINCIPAL).append(";");
            sb.append("user.keytab=").append(this.KERBEROS_KEYTAB_FILEPATH).append(";");
            if (!this.url.endsWith(";")) {
                this.url += ";";
            }
            this.url += sb.toString();
            if(!new File(this.KRB5_CONF).exists()){
                throw new RuntimeException("krb5.conf文件未找到:"+KRB5_CONF);
            }
            if(!new File(this.CORE_SITE).exists()){
                throw new RuntimeException("core-site.xml文件未找到:"+this.CORE_SITE);
            }
            if(!new File(this.HIVE_SITE).exists()){
                throw new RuntimeException("hive-site.xml文件未找到:"+this.HIVE_SITE);
            }
            if(!new File(this.KERBEROS_KEYTAB_FILEPATH).exists()){
                throw new RuntimeException("user.keytab文件未找到:"+this.KERBEROS_KEYTAB_FILEPATH);
            }
            System.setProperty("java.security.krb5.conf", this.KRB5_CONF);
            System.setProperty(ZOOKEEPER_SERVER_PRINCIPAL_KEY, ZOOKEEPER_DEFAULT_SERVER_PRINCIPAL);
            Configuration configuration = new Configuration();
            configuration.addResource(new Path(this.CORE_SITE));
            configuration.addResource(new Path(this.HIVE_SITE));
            System.setProperty("java.security.krb5.conf", this.KRB5_CONF);
            UserGroupInformation.setConfiguration(configuration);
            UserGroupInformation.loginUserFromKeytab(this.KERBEROS_PRINCIPAL,
                    this.KERBEROS_KEYTAB_FILEPATH);
        }
    }

    public HiveConfig(String url, String user, String password, boolean securityEnabled, String userPrincipal, Map<String, String> fileConfig) throws IOException {
        this.url = url;
        this.userName = user;
        this.password = password;
        if (securityEnabled) {
            StringBuilder sb = new StringBuilder();
            sb.append("user.principal=").append(userPrincipal).append(";");
            sb.append("user.keytab=").append(fileConfig.get("user.keytab")).append(";");
            if (!this.url.endsWith(";")) {
                this.url += ";";
            }
            this.url += sb.toString();
            if(!new File(fileConfig.get("krb5.conf")).exists()){
                throw new RuntimeException("krb5.conf文件未找到:"+fileConfig.get("krb5.conf"));
            }
            if(!new File(fileConfig.get("core-site.xml")).exists()){
                throw new RuntimeException("core-site.xml文件未找到:"+fileConfig.get("core-site.xml"));
            }
            if(!new File(fileConfig.get("hive-site.xml")).exists()){
                throw new RuntimeException("hive-site.xml文件未找到:"+fileConfig.get("hive-site.xml"));
            }
            if(!new File(fileConfig.get("user.keytab")).exists()){
                throw new RuntimeException("user.keytab文件未找到:"+fileConfig.get("user.keytab"));
            }
            System.setProperty("java.security.krb5.conf", fileConfig.get("krb5.conf"));
            System.setProperty(ZOOKEEPER_SERVER_PRINCIPAL_KEY, ZOOKEEPER_DEFAULT_SERVER_PRINCIPAL);
            Configuration configuration = new Configuration();
            configuration.addResource(new Path(fileConfig.get("core-site.xml")));
            configuration.addResource(new Path(fileConfig.get("hive-site.xml")));
            System.setProperty("java.security.krb5.conf", fileConfig.get("krb5.conf"));
            UserGroupInformation.setConfiguration(configuration);
            UserGroupInformation.loginUserFromKeytab(userPrincipal, fileConfig.get("user.keytab"));
//            UserGroupInformation loginUser = UserGroupInformation.getLoginUser();
        }
    }

    public String getUrl() {
        return url;
    }

    public String getUserName() {
        return userName;
    }

    public String getPassword() {
        return password;
    }
}
