package com.xmx.hadoop.mapreduce.partitioner;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowMapper extends Mapper<LongWritable, Text, Text, FlowBean> {

    Text k = new Text();

    FlowBean v = new FlowBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String str = value.toString();
        String[] values = str.split("\t");
        k.set(values[1]);
        v.setUpFlow(Long.parseLong(values[values.length - 3]));
        v.setDownFlow(Long.parseLong(values[values.length - 2]));
        v.setSumFlow();
        context.write(k, v);
    }
}
