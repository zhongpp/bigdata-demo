package com.zpp.hadoop.demo.kpi;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * 统计每个资源路径的ip访问MapReduce
 * map以资源路径为键，以ip为值输出
 * reduce完成以资源路径为键，ip统计值为值写入到结果文件中。
 * <p>
 * 完成的功能类似于单词统计！！
 */
public class KPIIP {

    public static class KPIIPMapper extends MapReduceBase implements Mapper<Object, Text, Text, Text> {
        private Text word = new Text();
        private Text ips = new Text();

        @Override
        public void map(Object key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            KPI kpi = KPI.filterIPs(value.toString());
            if (kpi.isValid()) {
                /**
                 * 以请求的资源路径为键
                 * 以ip为值
                 */
                word.set(kpi.getRequest());
                ips.set(kpi.getRemote_addr());
                output.collect(word, ips);
            }
        }
    }

    public static class KPIIPReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
        private Text result = new Text();
        private Set<String> count = new HashSet<String>();

        @Override
        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            /**
             * 以资源路径为键
             * 以相同资源路径的ip值的个数，添加入set集合中作为值
             * 写入到结果文件中
             */
            while (values.hasNext()) {
                count.add(values.next().toString());
            }
            //结果集中存放的是独立ip的个数
            result.set(String.valueOf(count.size()));
            System.out.println(key + " " + count.size());
            output.collect(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
//        String input = "hdfs://192.168.6.132:9000/test/access_2013_05_31.log";
//        String output = "hdfs://192.168.6.132:9000/out_kpiip3";
        JobConf conf = new JobConf(KPIIP.class);
        conf.setJobName("KPIIP");
        conf.addResource("classpath:core-site.xml");
        conf.addResource("classpath:hdfs-site.xml");
        conf.addResource("classpath:mapred-site.xml");
        conf.set("mapreduce.app-submission.cross-platform", "true");//意思是跨平台提交，在windows下如果没有这句代码会报错 "/bin/bash: line 0: fg: no job control"，去网上搜答案很多都说是linux和windows环境不同导致的一般都是修改YarnRunner.java，但是其实添加了这行代码就可以了。
        conf.set("mapreduce.framework.name", "yarn");//集群的方式运行，非本地运行。
        conf.set("mapred.jar", "D:\\IDEA\\bigdata-demo\\hadoop\\build\\libs\\hadoop-1.0-SNAPSHOT.jar");

        //conf.setMapOutputKeyClass(Text.class);
        //conf.setMapOutputValueClass(Text.class);

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        conf.setMapperClass(KPIIPMapper.class);
        conf.setCombinerClass(KPIIPReducer.class);
        conf.setReducerClass(KPIIPReducer.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        deleteDir(conf, args[1]);
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        JobClient.runJob(conf);
        System.exit(0);
    }

    /**
     * 删除指定目录
     *
     * @param conf
     * @param dirPath
     * @throws IOException
     */
    private static void deleteDir(Configuration conf, String dirPath) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path targetPath = new Path(dirPath);
        if (fs.exists(targetPath)) {
            boolean delResult = fs.delete(targetPath, true);
            if (delResult) {
                System.out.println(targetPath + " has been deleted sucessfullly.");
            } else {
                System.out.println(targetPath + " deletion failed.");
            }
        }

    }

}
