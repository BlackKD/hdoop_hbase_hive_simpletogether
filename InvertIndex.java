package org.apache.hadoop.count;

/**
 * Created by xiaopeng on 16/5/9.
 */
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class InvertIndex {

    public static int FileNum = 0;
    public static Configuration conf1 = null;
    static {
        conf1 = HBaseConfiguration.create();
//        conf.set("hbase.zookeeper.quorum", "node1");
//        conf.set("hbase.zookeeper.property.clientPort", "2181");
    }
    public static List<Put> putList = new ArrayList<Put>();
    public static FileWriter fw = null;


    public static class InvertedIndexMap extends Mapper<Object,Text,Text,Text>{

        private Text valueInfo = new Text();
        private Text keyInfo = new Text();
        private FileSplit split;

        public void map(Object key, Text value,Context context)
                throws IOException, InterruptedException {
            //获取<key value>对所属的FileSplit对象
            split = (FileSplit) context.getInputSplit();
            StringTokenizer stk = new StringTokenizer(value.toString());
            HTable table = new HTable(conf1, "stop");
            while (stk.hasMoreElements()) {
                //key值由（单词：URI）组成
                Text word = new Text();
                word.set(stk.nextToken());
                //word.set("千万");
                boolean find = false;
                Get g = new Get(Bytes.toBytes(word.toString()));
                Result r=table.get(g);
                for (KeyValue kv: r.raw()) {
                    if(word.toString().equals(new String(kv.getRow())))  {
                        //System.out.println("!!!!!!!!!!!!!!!!!!!!!!!!");
                        find = true;
                    }

                }

                if(find)
                {
                    continue;
                }

                keyInfo.set(word.toString()+":"+split.getPath().toString());
                //词频
                valueInfo.set("1");
                context.write(keyInfo, valueInfo);
            }
        }
    }

    public static class InvertedIndexCombiner extends Reducer<Text,Text,Text,Text>{

        Text info = new Text();

        public void reduce(Text key, Iterable<Text> values,Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (Text value : values) {
                sum += Integer.parseInt(value.toString());
            }

            int splitIndex = key.toString().indexOf(":");
            //重新设置value值由（URI+:词频组成）
            info.set(key.toString().substring(splitIndex+1) +":"+ sum);
            //重新设置key值为单词
            key.set(key.toString().substring(0,splitIndex));
            context.write(key, info);
        }
    }

    public static class InvertedIndexReduce extends Reducer<Text,Text,Text,Text>{

        private Text result = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            //生成文档列表
            String fileList = new String();
            int sum = 0;
            int times = 0;
            for (Text value : values) {
                String temp = value.toString();
                String [] str = temp.split(System.getProperty("file.separator"));
                String [] str2 = temp.split(":");
                String [] str1 = str[str.length - 1].split("\\.");
                sum += Integer.parseInt(str2[str2.length - 1]);
                String name = "";
                for (int i = 0; i < str1.length -2; i++) {
                    name += str1[i];
                }
                fileList += name + ":" + str2[str2.length - 1] + ";  ";
                times ++;
            }
            double average = (double)sum / (double)times;
            Put put = new Put(Bytes.toBytes(key.toString()));
            put.add(new String("apptimes").getBytes(), new String("times").getBytes(), new String(Double.toString(average)).getBytes());
            putList.add(put);
            fw.write(key + "\t" + fileList + "\n");
            String atemp = Double.toString(average)+ ", " + fileList;
            result.set(atemp);
            context.write(key, result);
        }
    }

    public static class InvertIndexSortMapper extends Mapper<Object,Text,DoubleWritable,Text> {
        private Text key = new Text();
        private FileSplit split;
        public void map(Object key, Text value, Mapper.Context context)
                throws IOException, InterruptedException {
            split = (FileSplit) context.getInputSplit();
            StringTokenizer stk = new StringTokenizer(value.toString());
            String name = stk.nextToken()+ " ";
            String temp = stk.nextToken().toString();
            String [] a = temp.split(",");
            while(stk.hasMoreTokens()) {
                name = name + stk.nextToken().toString() + " ";
            }
            double count = Double.parseDouble(a[0]);
            context.write(new DoubleWritable(count), new Text(name));
        }
    }

    public static class DoubleWritableDecreasingComparator extends DoubleWritable.Comparator {
        public int compare(WritableComparable a, WritableComparable b) {
            return -super.compare(a, b);
        }

        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return -super.compare(b1,s1,l1,b2,s2,l2);
        }
    }



    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        Configuration conf = new Configuration();
        File f = new File("Daopai.txt");
        fw = new FileWriter(f);
        FileSystem fs = FileSystem.get(conf);
        FileStatus[] inputFiles = fs.listStatus(new Path(args[0]));
        FileNum = inputFiles.length;
        Job job = new Job(conf,"InvertedIndex");
        conf1 = HBaseConfiguration.create();
        job.setJarByClass(InvertIndex.class);
        Path tempDir = new Path("count-temp-" + Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));
        try {

            job.setMapperClass(InvertedIndexMap.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            job.setCombinerClass(InvertedIndexCombiner.class);

            job.setReducerClass(InvertedIndexReduce.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, tempDir);
            if (job.waitForCompletion(true)) {
                HBaseAdmin admin = new HBaseAdmin(conf1);
                if(admin.tableExists("wuxia"))
                {
                    admin.disableTable("wuxia");
                    admin.deleteTable("wuxia");
                }
                HTableDescriptor  tableDesc = new HTableDescriptor("wuxia");
                tableDesc.addFamily(new HColumnDescriptor("apptimes"));
                admin.createTable(tableDesc);
                HTable table = new HTable(conf1,"wuxia");
                table.put(putList);
//                Job sortJob = new Job(conf, "sort");
//                sortJob.setJarByClass(InvertIndex.class);
//                FileInputFormat.addInputPath(sortJob, tempDir);
//                FileOutputFormat.setOutputPath(sortJob, new Path(args[1]));
//                sortJob.setSortComparatorClass(DoubleWritableDecreasingComparator.class);
//                sortJob.setMapperClass(InvertIndexSortMapper.class);
//                sortJob.setMapOutputKeyClass(DoubleWritable.class);
//                sortJob.setMapOutputValueClass(Text.class);
//                sortJob.setOutputKeyClass(DoubleWritable.class);
//                sortJob.setOutputValueClass(Text.class);
//                System.exit(sortJob.waitForCompletion(true) ? 0 : 1);
            }
        }
        finally {
            //FileSystem.get(conf).deleteOnExit(tempDir);
        }

    }
}