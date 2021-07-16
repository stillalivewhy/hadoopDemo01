package com.xmx.hadoop.mapreduce.partitioner;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class CustomPartitioner extends Partitioner<Text, FlowBean> {
    public int getPartition(Text text, FlowBean flowBean, int i) {
        String phone = text.toString();
        String prePhone = phone.substring(0, 3);
        int result;
        if("136".equals(prePhone)) {
            result = 0;
        } else if("137".equals(prePhone)) {
            result = 1;
        } else if("138".equals(prePhone)) {
            result = 2;
        } else if("139".equals(prePhone)) {
            result = 3;
        } else {
            result = 4;
        }
        return result;
    }
}
