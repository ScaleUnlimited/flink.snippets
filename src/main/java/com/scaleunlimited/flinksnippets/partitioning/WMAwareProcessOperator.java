package com.scaleunlimited.flinksnippets.partitioning;

import org.apache.flink.streaming.api.operators.ProcessOperator;
import org.apache.flink.streaming.api.watermark.Watermark;

/**
 * A ProcessOperator (one level below a ProcessFunction) that wraps a specific type of
 * ProcessFunction, in this case a WMAwareProcessFunction (Watermark Aware Process Function).
 *
 * @param <I>
 * @param <O>
 */
@SuppressWarnings("serial")
public class WMAwareProcessOperator<I, O> extends ProcessOperator<I, O> {

    public WMAwareProcessOperator(WMAwareProcessFunction<I, O> function) {
        super(function);
    }
    
    @Override
    public void processWatermark(Watermark mark) throws Exception {
        super.processWatermark(mark);
        
        // Whenever we get a watermark, call the ProcessFunction that we're wrapping
        ((WMAwareProcessFunction<I, O>)userFunction).processWatermark(mark, output);
    }
}
