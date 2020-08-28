package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class App {
    public static final String HBASE_ZK_PROP_MASTER = "hbase.master";
    public static final String HBASE_ZK_PROP_QUORUM = "hbase.zookeeper.quorum";
    public static final String HBASE_ZK_PROP_CLIENT_PORT = "hbase.zookeeper.property.clientPort";

    public static final String NAMESPACE = "pmt:";
    public static final String[] TABLES = {
            NAMESPACE + "bookmark-content",
            NAMESPACE + "ce-metrics-manifest",
            NAMESPACE + "hashtag-typeahead-suggestions",
            NAMESPACE + "last-activity-date",
            NAMESPACE + "lc-threadindex-v2",
            NAMESPACE + "malwareScanFileMapping",
            NAMESPACE + "malwareScanFileState",
            NAMESPACE + "malwareScanFileStateHistory",
            NAMESPACE + "s2-object",
            NAMESPACE + "session",
            NAMESPACE + "session-user-index",
            NAMESPACE + "symphony-attachments-streams",
            NAMESPACE + "symphony-blast",
            NAMESPACE + "symphony-cem",
            NAMESPACE + "symphony-cev",
            NAMESPACE + "symphony-delivery-record",
            NAMESPACE + "symphony-external-msg",
            NAMESPACE + "symphony-keystore-accountkeys-info",
            NAMESPACE + "symphony-keystore-cached-entities",
            NAMESPACE + "symphony-keystore-certs-info",
            NAMESPACE + "symphony-keystore-clientcerts",
            NAMESPACE + "symphony-keystore-contentkeys-info",
            NAMESPACE + "symphony-keystore-rsawrappedcontentkeys",
            NAMESPACE + "symphony-keystore-streams",
            NAMESPACE + "symphony-keystore-wrappedaccountkeys",
            NAMESPACE + "symphony-keystore-wrappedaccountkeys-v2",
            NAMESPACE + "symphony-keystore-wrappedcontentkeys",
            NAMESPACE + "symphony-last-read",
            NAMESPACE + "symphony-last-social-message",
            NAMESPACE + "symphony-message",
            NAMESPACE + "symphony-message-ef-violation",
            NAMESPACE + "symphony-message-hsm-failed",
            NAMESPACE + "symphony-message-receipts",
            NAMESPACE + "symphony-messageimport",
            NAMESPACE + "symphony-metadata-message",
            NAMESPACE + "symphony-notification",
            NAMESPACE + "symphony-presence",
            NAMESPACE + "symphony-relations",
            NAMESPACE + "symphony-release",
            NAMESPACE + "symphony-retention",
            NAMESPACE + "symphony-retention-streams",
            NAMESPACE + "symphony-retention-v2",
            NAMESPACE + "symphony-retention-v3",
            NAMESPACE + "symphony-session",
            NAMESPACE + "symphony-session-user-index",
            NAMESPACE + "symphony-stat",
            NAMESPACE + "symphony-user-receipts",
            NAMESPACE + "symphony-userthreadindex",
            NAMESPACE + "thread-query-throttling-table",
            NAMESPACE + "thread-query-throttling-table-ttl",
            NAMESPACE + "user-activethread-rotationid-index",
            NAMESPACE + "user-bookmark",
            NAMESPACE + "user-mentions",
            NAMESPACE + "user-mentions-rev",
            NAMESPACE + "usr-th-lm",
            NAMESPACE + "usrmsg",
            NAMESPACE + "xpod-key-request-retry",
            NAMESPACE + "xpod-retry",
            NAMESPACE + "xpod-retry-timing-info",
            NAMESPACE + "xpod_queue"
    };

    //TODO:
    // pass in --> quorum address
    //         --> client port

    public static Connection connection = null;
    public static Configuration hBaseConfig = null;

    public static void main(String[] args) throws Exception, IOException {
        ConnectionFactory.createConnection();

        hBaseConfig = HBaseConfiguration.create();
        hBaseConfig.setInt("timeout", 120000);
        //hBaseConfig.set(HBASE_ZK_PROP_MASTER, "172.18.0.11:16000");
        hBaseConfig.set(HBASE_ZK_PROP_QUORUM, "zookeeper");
        hBaseConfig.set(HBASE_ZK_PROP_CLIENT_PORT, "2181");

        HBaseAdmin.checkHBaseAvailable(hBaseConfig);
        System.out.println("HBase is running!");

        connection = ConnectionFactory.createConnection(hBaseConfig);

        try {
            HBaseAdmin admin1 = (HBaseAdmin) connection.getAdmin();
            System.out.println("stage 3. . . . ");
            HTableDescriptor tableDescriptor[] = admin1.listTables();


            System.out.println("Length:" + tableDescriptor.length);

            for (int i = 0; i < tableDescriptor.length; i++) {
                System.out.println(tableDescriptor[i].getNameAsString());
            }

            scanTableForRowkeyPrefix();

        } catch (Exception e) {
            System.out.println(e);
        }
    }

    public static void scanTableForRowkeyPrefix() throws IOException {

        for (String tableName:TABLES) {
            // Instantiating HTable class
            HTable table = new HTable(hBaseConfig, tableName);
            // Instantiating the Scan class
            //byte[] prefix = Bytes.toBytes("\\x9F\\x86\\x01\\x00");
            Scan scan = new Scan();
            //Filter prefixFilter = new PrefixFilter(prefix);
            ResultScanner resultScanner = table.getScanner(scan);
            printRows(resultScanner);
        }

    }

    public static void printRows(ResultScanner resultScanner) {
        for (Iterator<Result> iterator = resultScanner.iterator(); iterator.hasNext();) {
            printRow(iterator.next());
        }
    }

    public static void printRow(Result result) {
        String returnString = "";
        returnString += Bytes.toString(result.getValue(Bytes.toBytes("cf"), Bytes.toBytes("id"))) + ", ";
        returnString += Bytes.toString(result.getValue(Bytes.toBytes("cf"), Bytes.toBytes("type"))) + ", ";
        returnString += Bytes.toString(result.getValue(Bytes.toBytes("cf"), Bytes.toBytes("date")));
        System.out.println(returnString);
    }
}
