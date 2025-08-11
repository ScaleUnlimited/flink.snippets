package com.scaleunlimited.flinksnippets.watermarks;

import com.scaleunlimited.flinksnippets.MockSource;
import org.apache.flink.api.common.ExecutionMode;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.junit.jupiter.api.Test;

public class WatermarksWithBoundedSourcesTest {

    @Test
    public void testWatermarkGeneratedWithBoundedSourcesAndStreaming() throws Exception {
        // Create a bounded source with one record.
        SourceFunction<WatermarkedRecord> source = new MockSource<WatermarkedRecord>(
                new WatermarkedRecord(0)
        ).setNeverTerminate(false);

        // Create local stream environment w/parallelism of 2, event time
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(2);
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);

        env.addSource(source)
                .keyBy(r -> r.getId())
                .process(new PrintWatermarksFunction("watermarks"))
                .addSink(new DiscardingSink<>());

        env.execute();
    }
}
