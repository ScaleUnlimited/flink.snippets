package com.scaleunlimited.flinksnippets;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.util.Collector;

public class MyProcessWindows extends ProcessWindowFunction {

    @Override
    public void process(Object arg0, Context arg1, Iterable arg2, Collector arg3) throws Exception {
        // TODO Auto-generated method stub

    }

}
