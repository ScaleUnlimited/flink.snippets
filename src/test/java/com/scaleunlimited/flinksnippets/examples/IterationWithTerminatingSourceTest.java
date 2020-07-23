package com.scaleunlimited.flinksnippets.examples;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.junit.Test;

public class IterationWithTerminatingSourceTest {

    @Test
    public void test() throws Exception {
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.createLocalEnvironment(2);
        env.setParallelism(2);
        env.enableCheckpointing(100L, CheckpointingMode.AT_LEAST_ONCE, true);
        
        @SuppressWarnings("serial")
        RichParallelSourceFunction<Integer> source = new RichParallelSourceFunction<Integer>() {

            private boolean _keepRunning = true;
            private int _taskId;
            
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                
                _taskId = ((StreamingRuntimeContext)getRuntimeContext()).getIndexOfThisSubtask() * 10_000;
            }
            
            @Override
            public void run(SourceContext<Integer> ctx) throws Exception {
                ctx.collect(_taskId);
                
                long endTime = System.currentTimeMillis() + 1000L;
                while (_keepRunning && (System.currentTimeMillis() < endTime)) {
                    Thread.sleep(10L);
                }
                
                ctx.collect(-1);
                
                Thread.sleep(1000L);
            }

            @Override
            public void cancel() {
                _keepRunning = false;
            }
        };
        
        IterationWithTerminatingSource.build(env, source);
        env.execute();
    }

}
