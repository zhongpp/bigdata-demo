package com.zpp.hadoop.demo.kpi;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.Iterator;

/**
 * 浏览器统计MapReduce
 *
 */
public class KPIBrowser {

    public static class KPIBrowserMapper extends MapReduceBase implements Mapper<Object, Text, Text, IntWritable> {
        private IntWritable one = new IntWritable(1);
        private Text word = new Text();

        @Override
        public void map(Object key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
            KPI kpi = KPI.filterBroswer(value.toString());
            if (kpi.isValid()) {
            	
                word.set(kpi.getHttp_user_agent());
                output.collect(word, one);
            }
        }
    }

    public static class KPIBrowserReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        @Override
        public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
            int sum = 0;
            while (values.hasNext()) {
                sum += values.next().get();
            }
            result.set(sum);
            output.collect(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        String input = "hdfs://192.168.6.132:9000/test/access_2013_05_31.log";
        String output = "hdfs://92.168.6.132:9000/out_kpibrowser";

        JobConf conf = new JobConf(KPIBrowser.class);
        conf.setJobName("KPIBrowser");
        conf.addResource("classpath:core-site.xml");
        conf.addResource("classpath:hdfs-site.xml");
        conf.addResource("classpath:mapred-site.xml");
        
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);

        conf.setMapperClass(KPIBrowserMapper.class);
        conf.setCombinerClass(KPIBrowserReducer.class);
        conf.setReducerClass(KPIBrowserReducer.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(input));
        FileOutputFormat.setOutputPath(conf, new Path(output));
        //启动任务
        JobClient.runJob(conf);
        System.exit(0);
    }
}
