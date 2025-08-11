package com.scaleunlimited.flinksnippets.examples;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;

import com.scaleunlimited.flinksnippets.examples.PointOfCompromise.*;

class PointOfCompromiseTest {

    @Test
    void test() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(2);
        
        WatermarkStrategy<TransactionRecord> transactionsWatermarking = 
                WatermarkStrategy.<TransactionRecord>forBoundedOutOfOrderness(Duration.ofMinutes(1))
                .withTimestampAssigner((record, timestamp) -> record.getTransactionTime());
        
        DataStream<TransactionRecord> transactions = env.fromCollection(makeTransactions())
                .assignTimestampsAndWatermarks(transactionsWatermarking);
        
        // Add idleness for fraud reports, so transaction data will cause time to progress.
        WatermarkStrategy<FraudReport> fraudWatermarking =  
                WatermarkStrategy.<FraudReport>forBoundedOutOfOrderness(Duration.ofMinutes(1))
                .withTimestampAssigner((record, timestamp) -> record.getReportTime())
                .withIdleness(Duration.ofMinutes(1));

        DataStream<FraudReport> fraudReports = env.fromCollection(makeFraudReports())
                .assignTimestampsAndWatermarks(fraudWatermarking);
                
        FraudReportSink sink = new FraudReportSink();
        sink.reset();
        
        new PointOfCompromise()
                .setWindowSizeInDays(2)
                .setTransactions(transactions)
                .setFraudReports(fraudReports)
                .setResultSink(sink)
                .build();
        
        env.execute();
        
        System.out.println("Num results: " + sink.getSink().size());
        
        for (PoCResult result : sink.getSink()) {
            System.out.println(result);
        }
    }
    
    private List<TransactionRecord> makeTransactions() {
        List<TransactionRecord> result = new ArrayList<>();
        result.add(new TransactionRecord("transaction1", 0L, 100.0f, "vendor1", "card1"));
        // Use the same card at the same vendor, but 2 days from now, so watermark is generated that
        // will trigger generation of fraud report for vendor1 with a count of 1.
        result.add(new TransactionRecord("transaction2", Duration.ofDays(2).toMillis(), 10.0f, "vendor1", "card1"));
        return result;
    }
    
    private List<FraudReport> makeFraudReports() {
        List<FraudReport> result = new ArrayList<>();
        result.add(new FraudReport("card1", 1000L));
        return result;
    }

    @SuppressWarnings("serial")
    private static class FraudReportSink implements Sink<PoCResult> {

        private static final List<PoCResult> SINK = Collections.synchronizedList(new ArrayList<>());
        
        public void reset() {
            SINK.clear();
        }
        
        public List<PoCResult> getSink() {
            return SINK;
        }
        
        @Override
        public SinkWriter<PoCResult> createWriter(InitContext arg0) throws IOException {
            return new SinkWriter<PointOfCompromise.PoCResult>() {

                @Override
                public void close() throws Exception {}

                @Override
                public void flush(boolean arg0) throws IOException, InterruptedException {
                    // TODO Auto-generated method stub
                    
                }

                @Override
                public void write(PoCResult in, Context ctx) throws IOException, InterruptedException {
                    SINK.add(in);
                }
            };
        }
        
    }

}
