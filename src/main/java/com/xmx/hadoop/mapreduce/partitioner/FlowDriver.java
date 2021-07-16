package com.xmx.hadoop.mapreduce.partitioner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FlowDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(FlowDriver.class);
        //设置分区
        job.setPartitionerClass(CustomPartitioner.class);
        job.setNumReduceTasks(5);

        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);



        FileInputFormat.setInputPaths(job, new Path("E:\\大数据\\资料\\11_input\\inputflow"));
        FileOutputFormat.setOutputPath(job, new Path("E:\\test\\output0000"));

        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
