package com.alibaba.datax.plugin.writer.hbase11xwriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Hbase11xTest {
    private final static Logger LOG = LoggerFactory.getLogger(Hbase11xTest.class);

    public static void main(String[] args){

        try {
            System.setProperty("sun.security.krb5.debug","true");
            Configuration hConfiguration = HBaseConfiguration.create();
            //hConfiguration.set("hbase.zookeeper.property.clientPort","2181");
            String hbasesite = "/Users/haizhi/Downloads/hive/hbase-site.xml";

            hConfiguration.addResource(new Path(hbasesite));
            hConfiguration.addResource("/Users/haizhi/Downloads/hive/hdfs-site.xml");
            hConfiguration.addResource("/Users/haizhi/Downloads/hive/core-site.xml");
            //hConfiguration.set("hbase-site.xml","/Users/haizhi/Downloads/hive/hbase-site.xml");

            hConfiguration.set("hbase.zookeeper.quorum","cdh01:2181,cdh51401:2181,cdh51402:2181");
            hConfiguration.set("hbase.rootdir","/hbase");
            hConfiguration.set("hbase.cluster.distributed","true");

            String userKeytabFile = "/Users/haizhi/Downloads/hive/user.keytab";
            String krb5File = "/Users/haizhi/Downloads/hive/krb5.conf";
            String kerberosPrincipal = "dmp";

            System.setProperty("java.security.krb5.conf", krb5File);
            System.setProperty("zookeeper.server.principal", "zookeeper/hadoop");
            UserGroupInformation.setConfiguration(hConfiguration);
            UserGroupInformation.loginUserFromKeytab(kerberosPrincipal, userKeytabFile);

            Connection connection = ConnectionFactory.createConnection(hConfiguration);
            TableName tableName = TableName.valueOf("pre_datax2:te_address");
            Admin admin = connection.getAdmin();
            BufferedMutator bufferedMutator = connection.getBufferedMutator(tableName);
            if(!admin.tableExists(tableName)){
                System.out.println("表不存在");
            }
            if(!admin.isTableAvailable(tableName)){
                System.out.println("表不可用");
            }
            if(!admin.isTableDisabled(tableName)){
                System.out.println("表未启用");
            }
            /*Table table = connection.getTable(tableName);
            ResultScanner results = table.getScanner(new Scan());
            for(Result result:results){
                System.out.println(result.getRow());
            }*/
        }catch (Exception e){
            System.out.println(e);
        }



    }
}
