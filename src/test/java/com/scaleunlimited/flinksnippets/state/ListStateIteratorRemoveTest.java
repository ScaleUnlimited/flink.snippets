package com.scaleunlimited.flinksnippets.state;

import java.io.File;
import java.util.Iterator;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.util.Collector;
import org.junit.Test;

public class ListStateIteratorRemoveTest {

    @Test
    public void testListStateRemoveWithInMemoryState() throws Exception {
        File rocksDBcheckpointDataDir = new File("./target/ListStateIteratorRemoveTest/checkpoints/");
        rocksDBcheckpointDataDir.mkdirs();
        
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);
        env.setStateBackend(new RocksDBStateBackend(rocksDBcheckpointDataDir.toURI()));
        
        env.addSource(new GenerateIntegersSource())
            .keyBy(r -> r)
            .process(new TestListState())
            .addSink(new DiscardingSink<>());
        
        env.execute();
    }
    
    @SuppressWarnings("serial")
    private static class GenerateIntegersSource extends RichSourceFunction<Integer> {

        private transient volatile boolean running;
        
        @Override
        public void run(SourceContext<Integer> ctx) throws Exception {
            running = true;
            
            int curInt = 0;
            while (running && (curInt <= 10)) {
                ctx.collect(curInt);
                curInt++;
                
                Thread.sleep(10);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
    
    @SuppressWarnings("serial")
    private static class TestListState extends KeyedProcessFunction<Integer, Integer, Void> {

        private transient ListState<Integer> state;
        
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            
            state = getRuntimeContext().getListState(new ListStateDescriptor<>("state", Integer.class));
        }
        
        @Override
        public void processElement(Integer in, Context ctx, Collector<Void> out) throws Exception {
            if (in < 10) {
                state.add(in);
            } else if (in == 10) {
                Iterator<Integer> iter = state.get().iterator();
                while (iter.hasNext()) {
                    iter.next();
                    iter.remove();
                }
            } else {
                if (state.get().iterator().hasNext()) {
                    throw new RuntimeException("Iterator.remove didn't remove!");
                }
            }
        }
        
    }
}
