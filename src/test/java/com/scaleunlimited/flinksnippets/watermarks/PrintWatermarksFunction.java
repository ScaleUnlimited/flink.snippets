package com.scaleunlimited.flinksnippets.watermarks;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

@SuppressWarnings("serial")
public class PrintWatermarksFunction extends ProcessFunction<WatermarkedRecord, WatermarkedRecord> {

    private String prefix;

    private transient int subtaskIndex;

    public PrintWatermarksFunction(String prefix) {
        this.prefix = prefix;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();
    }

    @Override
    public void processElement(WatermarkedRecord in, Context ctx, Collector<WatermarkedRecord> out) throws Exception {
        String watermark = convertWatermark(ctx.timerService().currentWatermark());

        System.out.format("%s [%d] id %s: timestamp %d received at watermark %s\n", prefix, subtaskIndex, in.getId(), in.getTimestamp(), watermark);

        ctx.timerService().registerEventTimeTimer(in.getTimestamp());
        out.collect(in);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<WatermarkedRecord> out) throws Exception {
        String watermark = convertWatermark(ctx.timerService().currentWatermark());
        System.out.format("%s [%d] timestamp %d fired at watermark %s\n", prefix, subtaskIndex, timestamp, watermark);
    }

    private String convertWatermark(long watermark) {
        if (watermark == Long.MIN_VALUE) {
            return "UNINITIALIZED";
        } else if (watermark == Long.MAX_VALUE) {
            return "MAX_VALUE";
        } else {
            return Long.toString(watermark);
        }
    }
}
