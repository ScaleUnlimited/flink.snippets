package com.scaleunlimited.flinksnippets.watermarks;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.junit.Test;

import com.scaleunlimited.flinksnippets.MockSource;

public class WatermarkTest {

    // See https://ci.apache.org/projects/flink/flink-docs-stable/dev/event_timestamps_watermarks.html#how-operators-process-watermarks
    
    @Test
    public void testWatermarkAfterUnion() throws Exception {
        // Create empty source, so no watermarks.
        SourceFunction<WatermarkedRecord> source1 = new MockSource<WatermarkedRecord>(TypeInformation.of(WatermarkedRecord.class))
                .setNeverTerminate(true)
                .setDelay(Time.milliseconds(10));
        
        // Create source with 6 different records, in increasing time.
        SourceFunction<WatermarkedRecord> source2 = new MockSource<WatermarkedRecord>(
                        new WatermarkedRecord(0),
                        new WatermarkedRecord(1),
                        new WatermarkedRecord(2),
                        new WatermarkedRecord(3),
                        new WatermarkedRecord(4),
                        new WatermarkedRecord(5))
                .setNeverTerminate(true)
                .setDelay(Time.milliseconds(10));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);

        // Create a watermark strategy that emits a watermark with every record, with the
        // watermark time equal to the record event time.
        WatermarkStrategy<WatermarkedRecord> ws = (ctx -> new WatermarkWithEveryRecord());
        ws = ws.withTimestampAssigner((r, ts) -> r.getTimestamp());
        
        DataStream<WatermarkedRecord> stream1 = env.addSource(source1)
                .assignTimestampsAndWatermarks(ws)
                .process(new PrintWatermarksFunction("stream1"));
        
        DataStream<WatermarkedRecord> stream2 = env.addSource(source2)
                .assignTimestampsAndWatermarks(ws)
                .process(new PrintWatermarksFunction("stream2"));
            
        stream1.union(stream2).process(new PrintWatermarksFunction("unioned"))
        .addSink(new DiscardingSink<>());
        
        env.execute();
    }
    
    
    @Test
    public void testWatermarkAfterKeyBy() throws Exception {
        
        SourceFunction<WatermarkedRecord> source = new MockSource<WatermarkedRecord>(
                new WatermarkedRecord(0),
                new WatermarkedRecord(1),
                new WatermarkedRecord(2),
                new WatermarkedRecord(3),
                new WatermarkedRecord(4),
                new WatermarkedRecord(5)
        ).setDelay(Time.milliseconds(10));
        
        // Create local stream environment w/parallelism of 2, event time
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(2);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(1);
        
        // This is the weird bit of the new (1.11) Flink API for watermarks. You need to
        // provide a watermark strategy that implements the createWatermarkGenerator(ctx)
        // method, which returns a WatermarkGenerator. But even though a WatermarkStrategy
        // is an interface, it uses the Java 8 "default" keyword to define a createTimestampAssigner()
        // method that just returns the timestamp assigned to a record (or Long.MIN_VALUE, if
        // unassigned). You override that by calling the ws.withTimestampAssigner() method
        // with a TimestampAssigner (or lambda function, as per below).
        WatermarkStrategy<WatermarkedRecord> ws = (ctx -> new MyWatermarkGenerator());
        ws = ws.withTimestampAssigner((r, ts) -> r.getTimestamp());
        
        env.addSource(source)
            .assignTimestampsAndWatermarks(ws)
            .process(new PrintWatermarksFunction("prekey"))
            .keyBy(r -> String.format("%d",  r.getId()))
            .process(new PrintWatermarksFunction("postkey"))
            .addSink(new DiscardingSink<>());
        
        env.execute();
    }
    
    private static class MyWatermarkGenerator implements WatermarkGenerator<WatermarkedRecord> {

        private long timestamp = -1;
        private long lastWatermark = Long.MIN_VALUE;
        private int lastId = -1;

        @Override
        public void onEvent(WatermarkedRecord in, long eventTimestamp, WatermarkOutput out) {
            timestamp = in.getTimestamp();
            lastId = in.getId();
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput out) {
            if (lastWatermark != timestamp) {
                System.out.format("watermark [%d]: %d\n", lastId % 2, timestamp);

                out.emitWatermark(new Watermark(timestamp));

                lastWatermark = timestamp;
            }
        }
    }
    
    private static class WatermarkWithEveryRecord implements WatermarkGenerator<WatermarkedRecord> {

        @Override
        public void onEvent(WatermarkedRecord in, long eventTimestamp, WatermarkOutput out) {
            System.out.format("Emitting watermark for id %d at %d\n", in.getId(), in.getTimestamp());
            out.emitWatermark(new Watermark(in.getTimestamp()));
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            // Do nothing on period call
        }
    }

}
