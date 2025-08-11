package com.scaleunlimited.flinksnippets.partitioning;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Collector;

/**
 * This is an abstract class that extends a non-keyed ProcessFunction, by adding a method
 * that will be called by the MWAwareProcessOperator whenever it receives a watermark record.
 *
 * @param <I> Input type
 * @param <O> Output type
 */
@SuppressWarnings("serial")
public abstract class WMAwareProcessFunction<I, O> extends ProcessFunction<I, O> {

    public abstract void processWatermark(Watermark watermark, Collector<StreamRecord<O>> out);
    
}
