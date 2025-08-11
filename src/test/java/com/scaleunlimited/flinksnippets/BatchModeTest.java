package com.scaleunlimited.flinksnippets;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;

import org.apache.flink.api.common.ExecutionMode;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.lib.NumberSequenceSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.Test;


class BatchModeTest {

    private static final int PRODUCER_PARALLELISM = 1;
    private static final int CONSUMER_PARALLELISM = 1;

    private static final int NUM_TASKS = 16 * 5 * 100000;
    private static final double TIME_UNIT = 0.5;

    @Test
    void test() throws Exception {
        Configuration config = new Configuration();
                config.setString("restart-strategy.type", "fixed-delay");
                config.setString("restart-strategy.fixed-delay.attempts", "3");
                config.setString("restart-strategy.fixed-delay.delay", "10000ms");
                config.setString("state.backend.type", "hashmap");
                config.setString(
                    "state.checkpoints.dir",
                    "file:///Users/kenkrugler/Downloads/flink/flink-checkpoints");
                config.set(RestOptions.PORT, 8081);
                
       StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config);
       env = env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
       env.getCheckpointConfig().enableUnalignedCheckpoints();
       env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
       
       NumberSequenceSource source = new NumberSequenceSource(0, NUM_TASKS);
               DataStream<Long> ds = env.fromSource(source,
                       WatermarkStrategy.forMonotonousTimestamps(),
                       "file_source",
                       Types.LONG).setParallelism(1);
               
               ds = ds.keyBy(x -> x % PRODUCER_PARALLELISM);
               ds.flatMap(new Producer()).setParallelism(PRODUCER_PARALLELISM);
               
               env.execute("Example");

    }
    
    
    @SuppressWarnings("serial")
    private static class Producer extends RichFlatMapFunction<Long, Long> {

        private transient String taskInfo;
        private transient int taskIndex;
        private transient int attemptNumber;
        private transient ValueState<Integer> count;
        
        @Override
        public void open(Configuration parameters) throws Exception {
                    getRuntimeContext().getTaskNameWithSubtasks();
                    taskIndex = getRuntimeContext().getIndexOfThisSubtask();
                    attemptNumber = getRuntimeContext().getAttemptNumber();

                    ValueStateDescriptor<Integer> descriptor = new ValueStateDescriptor<>(
                        "average",
                        Types.INT);
                    count = getRuntimeContext().getState(descriptor);

        }
        
        @Override
        public void flatMap(Long in, Collector<Long> out) throws Exception {
            Integer curValue = count.value();
            if (curValue == null) {
                curValue = 0;
            }
            System.out.format("count: %d\n", curValue);

            count.update(curValue + 1);
            
//            Thread.sleep(500);
//            int cnt = 0;
//            while (cnt < 10000) {
//                cnt += 1;
//            }

            out.collect(in);
        }
        
        
    }
    
    

}
