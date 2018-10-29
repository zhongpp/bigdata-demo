package com.zpp.hadoop.demo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import java.io.IOException;
import java.util.StringTokenizer;
/**
 * Description:
 *
 * @Author: weishenpeng
 * Date: 2017/11/14
 * Time: 下午 12:31
 */
public class WordCount3 {
    public static class WcMap extends Mapper {
        private final IntWritable one = new IntWritable(1);
        private Text word = new Text();
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            StringTokenizer token = new StringTokenizer(line);
            while (token.hasMoreTokens()) {
                word.set(token.nextToken());
                context.write(word, one);
            }
        }
    }
    public static class WcReduce extends Reducer {
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }
    public static void main(String[] args) throws Exception {
        System.setProperty("hadoop.home.dir", "D:\\hadoop-2.9.1");
//System.setProperty("hadoop.home.dir", "//opt//soft//bdq//hadoop-2.7.1");
        Configuration configuration = new Configuration();
        Job job = new Job(configuration);
        job.setJarByClass(WordCount.class);
        job.setJobName("wordCount");
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setMapperClass(WcMap.class);
        job.setReducerClass(WcReduce.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path("hdfs://192.168.6.132:9000/test/test.txt"));
        FileOutputFormat.setOutputPath(job, new Path("hdfs://192.168.6.132:9000/test/out11/RESULT.log"));
        job.waitForCompletion(true);
    }
}