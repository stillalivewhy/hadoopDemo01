package com.xmx.hadoop.mapreduce.partitioner;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowReducer extends Reducer<Text, FlowBean, Text, FlowBean> {

    private FlowBean v = new FlowBean();

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {

        Long upFlow = 0l;
        Long downFlow = 0l;

        for (FlowBean value : values) {
            upFlow += value.getUpFlow();
            downFlow += value.getDownFlow();
        }

        v.setUpFlow(upFlow);
        v.setDownFlow(downFlow);
        v.setSumFlow();
        context.write(key, v);
    }
}
