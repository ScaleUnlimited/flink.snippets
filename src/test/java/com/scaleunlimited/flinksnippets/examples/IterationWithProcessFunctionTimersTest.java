package com.scaleunlimited.flinksnippets.examples;

import java.io.Serializable;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IterationWithProcessFunctionTimersTest {
    static final Logger LOGGER = LoggerFactory.getLogger(IterationWithProcessFunctionTimersTest.class);

    @Test
    public void test() throws Exception {
        System.setProperty("my.root.level", "INFO");
        
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.createLocalEnvironment(2);
        env.setParallelism(2);
        env.enableCheckpointing(100L, CheckpointingMode.AT_LEAST_ONCE, true);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(100L);
        
        final TimedTerminator terminator = new TimedTerminator(1000);
        
        @SuppressWarnings("serial")
        RichParallelSourceFunction<String> source = new RichParallelSourceFunction<String>() {

            private boolean _keepRunning = true;
            private int _taskId;
            
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                
                terminator.open();
                
                _taskId = ((StreamingRuntimeContext)getRuntimeContext()).getIndexOfThisSubtask();
            }
            
            @Override
            public void run(SourceContext<String> ctx) throws Exception {
                int domainId = _taskId * 1000;
                int numDomains = 0;
                
                while (_keepRunning && !terminator.isStopTime() && (numDomains < 10)) {
                    LOGGER.info("Emitting domain{}", domainId);
                    ctx.collect("domain" + domainId + ":");
                    domainId += 1;
                    numDomains += 1;
                    Thread.sleep(10L);
                }
            }

            @Override
            public void cancel() {
                _keepRunning = false;
            }
        };
        
        IterationWithProcessFunctionTimers.build(env, source, new TimedTerminator(1000));
        env.execute();
    }
    
    @SuppressWarnings("serial")
    public static class TimedTerminator implements Serializable {
        
        private long _duration;
        
        private transient long _endTime;
        
        public TimedTerminator(long duration) {
            _duration = duration;
        }
        
        public void open() {
            _endTime = System.currentTimeMillis() + _duration;
        }
        
        public boolean isStopTime() {
            return System.currentTimeMillis() >= _endTime;
        }
    }

}
