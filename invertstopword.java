package org.apache.hadoop.count;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.rest.protobuf.generated.ScannerMessage;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
/**
 * Created by xiaopeng on 16/5/10.
 */
public class invertstopword {
    private static Configuration conf1 = null;
    private static FileWriter fileWriter = null;
    public static List<Put> putList = new ArrayList<Put>();
    static {
        conf1 = HBaseConfiguration.create();
//        conf.set("hbase.zookeeper.quorum", "node1");
//        conf.set("hbase.zookeeper.property.clientPort", "2181");
    }
    public static void main(String[] args) throws IOException {
        File targeFile = new File("Stop_words.txt");
        Scanner input = new Scanner(targeFile);
        try {
            while(input.hasNext())
            {
                //double average = (double)sum / (double)times;
                Put put = new Put(Bytes.toBytes(input.nextLine().toString()));
                //put.add(new String("name").getBytes());
                //put.add
                put.add(new String("name").getBytes(), new String("times").getBytes(), put.toString().getBytes());
                putList.add(put);
            }
            //Configuration conf = new Configuration();
            //conf1 = HBaseConfiguration.create();
            HBaseAdmin admin = new HBaseAdmin(conf1);
            if(admin.tableExists("stop"))
            {
                admin.disableTable("stop");
                admin.deleteTable("stop");
            }
            HTableDescriptor tableDesc = new HTableDescriptor("stop");
            tableDesc.addFamily(new HColumnDescriptor("name"));
            admin.createTable(tableDesc);
            HTable table = new HTable(conf1,"stop");
            table.put(putList);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
