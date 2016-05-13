package org.apache.hadoop.count;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

import java.io.FileWriter;
import java.io.IOException;
import java.io.File;

/**
 * Created by jiangchenzhou on 16/5/8.
 */
public class scanWuxiaTable {
    private static Configuration conf = null;
    private static FileWriter fileWriter = null;
    static {
        conf = HBaseConfiguration.create();
//        conf.set("hbase.zookeeper.quorum", "node1");
//        conf.set("hbase.zookeeper.property.clientPort", "2181");
    }

    public static void main(String[] args) throws IOException {
        File f = new File("wuxia.txt");
        fileWriter = new FileWriter(f);
        HTable table = new HTable(conf, "wuxia");
        Scan s = new Scan();
        ResultScanner ss = table.getScanner(s);
        try {
            for (Result r: ss) {
                for (KeyValue kv: r.raw()) {
                    System.out.print("row:; " + new String(kv.getRow()) + " ");
                    System.out.print("family: " + new String(kv.getFamily()) + " ");
                    System.out.print("qualifier: " + new String(kv.getQualifier()) + " ");
                    System.out.println("value: " + new String(kv.getValue()));
                    fileWriter.write(new String(kv.getRow()) + "\t");
                    fileWriter.write(new String(kv.getValue()) + "\n");
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
