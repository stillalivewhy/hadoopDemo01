package com.xmx.hadoop.mapreduce.partitioner;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FlowBean implements Writable {

    private Long upFlow;

    private Long downFlow;

    private Long sumFlow;

    public Long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(Long upFlow) {
        this.upFlow = upFlow;
    }

    public Long getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(Long downFlow) {
        this.downFlow = downFlow;
    }

    public Long getSumFlow() {
        return sumFlow;
    }

    public void setSumFlow() {
        this.sumFlow = this.upFlow + this.downFlow;
    }

    public FlowBean() {
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(this.upFlow);
        dataOutput.writeLong(this.downFlow);
        dataOutput.writeLong(this.sumFlow);
    }

    public void readFields(DataInput dataInput) throws IOException {
        this.upFlow = dataInput.readLong();
        this.downFlow = dataInput.readLong();
        this.sumFlow = dataInput.readLong();
    }

    @Override
    public String toString() {
        return this.upFlow + "\t" + this.downFlow + "\t" + this.sumFlow;
    }
}
