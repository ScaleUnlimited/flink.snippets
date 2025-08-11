package com.scaleunlimited.flinksnippets;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.util.CloseableIterator;
import static org.junit.Assert.*;
import org.junit.Test;

public class ObjectReuseTest {
    
    @Test
    public void testObjectReuse() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);
        env.getConfig().enableObjectReuse();
        
        DataStream<Event> stream1 = env.fromElements(
                new Event("A", 1));
        
        stream1.map((Event r) -> {
                r.setValue(r.getValue() * 2);
                return r;
            })
            .addSink(new DiscardingSink<>());
        
        DataStream<Event> stream2 = stream1.map(r -> r);
        
        CloseableIterator<Event> results = stream2.collectAsync();
               
        env.execute();
        
        assertTrue(results.hasNext());
        Event result = results.next();
        assertEquals(1, result.getValue());
        assertFalse(results.hasNext());
    }
    
    public static class Event {
        private String label;
        private long value;
        
        public Event() {}
        
        public Event(String label, long value) {
            this.label = label;
            this.value = value;
        }

        public String getLabel() {
            return label;
        }

        public void setLabel(String label) {
            this.label = label;
        }

        public long getValue() {
            return value;
        }

        public void setValue(long value) {
            this.value = value;
        }
    }
    
}
