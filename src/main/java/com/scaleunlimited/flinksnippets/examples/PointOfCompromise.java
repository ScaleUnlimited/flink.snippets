package com.scaleunlimited.flinksnippets.examples;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Find vendors with the max number of fraud transactions reported
 * by cards used at that vendor within N days of the fraud report
 */
public class PointOfCompromise {

    private static final int DEFAULT_WINDOW_SIZE = 14;
    
    private int windowSizeInDays = DEFAULT_WINDOW_SIZE;
    private DataStream<TransactionRecord> transactions;
    private DataStream<FraudReport> fraudReports;
    private Sink<PoCResult> resultSink;

    public PointOfCompromise setWindowSizeInDays(int windowSizeInDays) {
        this.windowSizeInDays = windowSizeInDays;
        return this;
    }

    public PointOfCompromise setTransactions(DataStream<TransactionRecord> transactions) {
        this.transactions = transactions;
        return this;
    }

    public PointOfCompromise setFraudReports(DataStream<FraudReport> fraudReports) {
        this.fraudReports = fraudReports;
        return this;
    }

    public PointOfCompromise setResultSink(Sink<PoCResult> result) {
        this.resultSink = result;
        return this;
    }

    public PointOfCompromise build() {
        Preconditions.checkNotNull(transactions, "Transactions DataStream must be set");
        Preconditions.checkNotNull(fraudReports, "Fraud Reports DataStream must be set");
        Preconditions.checkNotNull(resultSink, "Results Sink must be set");
        
        // In the transaction stream, group by card, window by N days with a slide
        // of 1 day, and generate a list of unique vendorIds per card/time window.
        DataStream<VendorsPerCardWindow> vendorsPerCardWindow = transactions
            .keyBy(r -> r.getCardNumber())
            .window(SlidingEventTimeWindows.of(Time.days(windowSizeInDays), Time.days(1)))
            .aggregate(new AggregateUniqueCards(), new CreateVendorsPerCardWindow())
            .keyBy(r -> r.getCardNumber());
            
        // Join this stream against the fraud reports, using the card number.
        // If we get no fraud report by the end of the window time, purge the
        // transaction set. If we get a join, output vendorId/window time
        // for each vendor.
        DataStream<Tuple2<String, Long>> vendorFraud = fraudReports
            .keyBy(r -> r.getCardNumber())
            .connect(vendorsPerCardWindow)
            .process(new JoinFraudAndTransactions(windowSizeInDays));
            

        // Group by vendor and window time, count records.
        DataStream<Tuple3<String, Long, Integer>> fraudPerVendorWindow = vendorFraud
            .keyBy(new KeySelector<Tuple2<String,Long>, Tuple2<String,Long>>() {

                @Override
                public Tuple2<String, Long> getKey(Tuple2<String, Long> in) throws Exception {
                    return in;
                }
                
            })
            .window(SlidingEventTimeWindows.of(Time.days(windowSizeInDays), Time.days(1)))
            .aggregate(new CountFraudPerVendorWindow(), new AssignWindowTime());
        
        
        // Group by window time, find max vendor/count, send to sink
        fraudPerVendorWindow
            .keyBy(t -> t.f1)
            .window(SlidingEventTimeWindows.of(Time.days(windowSizeInDays), Time.days(1)))
            .max(2)
            .map(t -> new PoCResult(t.f1, t.f0, t.f2))
            .sinkTo(resultSink);
        
        return this;
    }
    
    @SuppressWarnings("serial")
    private static class AssignWindowTime extends ProcessWindowFunction<Integer, Tuple3<String, Long, Integer>, Tuple2<String, Long>, TimeWindow> {

        @Override
        public void process(Tuple2<String, Long> key, Context ctx,
                Iterable<Integer> iter, Collector<Tuple3<String, Long, Integer>> out) throws Exception {
            out.collect(Tuple3.of(key.f0, key.f1, iter.iterator().next()));
        }
        
    }
    
    @SuppressWarnings("serial")
    private static class JoinFraudAndTransactions extends KeyedCoProcessFunction<String, FraudReport, VendorsPerCardWindow, Tuple2<String, Long>> {

        private static final long MS_PER_DAY = Duration.ofDays(1).toMillis();
        
        private long windowSizeInMS;
        
        private transient MapState<Long, FraudReport> fraudReports;
        private transient MapState<Long, VendorsPerCardWindow> vendorsInWindow;
        
        public JoinFraudAndTransactions(int windowSizeInDays) {
            this.windowSizeInMS = windowSizeInDays * MS_PER_DAY;
        }
        
        @Override
        public void open(Configuration parameters) throws Exception {
            MapStateDescriptor<Long, FraudReport> msd1 = new MapStateDescriptor<>("frauds", Long.class, FraudReport.class);
            fraudReports = getRuntimeContext().getMapState(msd1);
            
            MapStateDescriptor<Long, VendorsPerCardWindow> msd2 = new MapStateDescriptor<>("vendors", Long.class, VendorsPerCardWindow.class);
            vendorsInWindow = getRuntimeContext().getMapState(msd2);
        }
        
        @Override
        public void processElement1(FraudReport in, Context ctx,
                Collector<Tuple2<String, Long>> out) throws Exception {
            // Save the fraud report in our map state (keyed by fraud time),
            // and set a timer to clear it out that's equal to the window
            // size (so if a fraud report came in at the same time as a transaction,
            // we'd keep it around until that transaction's window is complete).
            
            long reportTime = in.getReportTime();
            fraudReports.put(reportTime, in);
            ctx.timerService().registerEventTimeTimer(in.getReportTime() + windowSizeInMS);
            
            // If we have any pending vendors for this card, see if this fraud
            // report time is inside of their window. If so, generate results.
            for (Entry<Long, VendorsPerCardWindow> vendors : vendorsInWindow.entries()) {
                long windowEnd = vendors.getKey();
                long windowStart = windowEnd + 1 - windowSizeInMS;
                if ((reportTime >= windowStart) && (reportTime <= windowEnd)) {
                    // Generate results <vendorId, windowEnd>
                    for (String vendorId : vendors.getValue().getVendorIds()) {
                        out.collect(Tuple2.of(vendorId, windowEnd));
                    }
                    
                    // TODO remove entry from map
                    // TODO remove timer
                }
            }
        }

        @Override
        public void processElement2(VendorsPerCardWindow in, Context ctx,
                Collector<Tuple2<String, Long>> out) throws Exception {
            // TODO If we have a fraud report for this incoming window,
            // generate the result.
            long windowEnd = in.getWindowTime();
            long windowStart = windowEnd + 1 - windowSizeInMS;
            for (Entry<Long, FraudReport> fraudReport : fraudReports.entries()) {
                long reportTime = fraudReport.getKey();
                if ((reportTime >= windowStart) && (reportTime <= windowEnd)) {
                    // Generate results <vendorId, windowEnd>
                    for (String vendorId : in.getVendorIds()) {
                        out.collect(Tuple2.of(vendorId, windowEnd));
                    }
                    
                    // We could have multiple fraud reports (on same day, or
                    // on different days within the window). But all we care
                    // about is that for this window. we generate a vendor id.
                    // Otherwise we would be over-counting the number of cards
                    // used at vendor X in window Y.
                    return;
                }
            }

            // If not, then save in our MapState<window end time, record>
            // and create a timer. We want the timer to fire when we get
            // a watermark _after_ the end of the window.
            vendorsInWindow.put(in.getWindowTime(), in);
            ctx.timerService().registerEventTimeTimer(in.getWindowTime() + 1);
        }
        
        @Override
        public void onTimer(long timestamp, OnTimerContext ctx,
                Collector<Tuple2<String, Long>> out) throws Exception {
            // TODO Clear out state. Could be fraud and/or vendors
        }
        
    }
    
    
    
    
    public static class TransactionRecord {
        private String transactionId;
        private long transactionTime;
        private float transactionAmount;
        private String vendorId;
        private String cardNumber;
        
        public void TransactionRecord() {}

        public TransactionRecord(String transactionId, long transactionTime, float transactionAmount, String vendorId,
                String cardNumber) {
            this.transactionId = transactionId;
            this.transactionTime = transactionTime;
            this.transactionAmount = transactionAmount;
            this.vendorId = vendorId;
            this.cardNumber = cardNumber;
        }

        public String getTransactionId() {
            return transactionId;
        }

        public void setTransactionId(String transactionId) {
            this.transactionId = transactionId;
        }

        public long getTransactionTime() {
            return transactionTime;
        }

        public void setTransactionTime(long transactionTime) {
            this.transactionTime = transactionTime;
        }

        public float getTransactionAmount() {
            return transactionAmount;
        }

        public void setTransactionAmount(float transactionAmount) {
            this.transactionAmount = transactionAmount;
        }

        public String getVendorId() {
            return vendorId;
        }

        public void setVendorId(String vendorId) {
            this.vendorId = vendorId;
        }

        public String getCardNumber() {
            return cardNumber;
        }

        public void setCardNumber(String cardNumber) {
            this.cardNumber = cardNumber;
        }
        
        @Override
        public String toString() {
            return String.format("Card %s used at vendor %s on %s for %f", 
                    cardNumber, vendorId, new Date(transactionTime), transactionAmount);
        }
    }
    
    public static class FraudReport {
        private String cardNumber;
        private long reportTime;
        
        public void FraudReport() {}

        public FraudReport(String cardNumber, long reportTime) {
            this.cardNumber = cardNumber;
            this.reportTime = reportTime;
        }

        public String getCardNumber() {
            return cardNumber;
        }

        public void setCardNumber(String cardNumber) {
            this.cardNumber = cardNumber;
        }

        public long getReportTime() {
            return reportTime;
        }

        public void setReportTime(long reportTime) {
            this.reportTime = reportTime;
        }
    }

    public static class PoCResult {
        private long windowTime;
        private String vendorId;
        private int numCards;
        
        public PoCResult () {}

        public PoCResult(long windowTime, String vendorId, int numCards) {
            this.windowTime = windowTime;
            this.vendorId = vendorId;
            this.numCards = numCards;
        }

        public long getWindowTime() {
            return windowTime;
        }

        public void setWindowTime(long windowTime) {
            this.windowTime = windowTime;
        }

        public String getVendorId() {
            return vendorId;
        }

        public void setVendorId(String vendorId) {
            this.vendorId = vendorId;
        }

        public int getNumCards() {
            return numCards;
        }

        public void setNumCards(int numCards) {
            this.numCards = numCards;
        }
        
        @Override
        public String toString() {
            return String.format("Vendor %s had %d cards with fraud reports during %d", vendorId, numCards, windowTime);
        }
    }
    
    private static class VendorsPerCardWindow {
        private String cardNumber;
        private long windowTime;
        // TODO serialization of this field
        private List<String> vendorIds;
        
        public VendorsPerCardWindow() {}

        public VendorsPerCardWindow(String cardNumber) {
            this.cardNumber = cardNumber;
        }

        public String getCardNumber() {
            return cardNumber;
        }

        public void setCardNumber(String cardNumber) {
            this.cardNumber = cardNumber;
        }

        public long getWindowTime() {
            return windowTime;
        }

        public void setWindowTime(long windowTime) {
            this.windowTime = windowTime;
        }

        public List<String> getVendorIds() {
            return vendorIds;
        }

        public void setVendorIds(List<String> vendorIds) {
            this.vendorIds = vendorIds;
        }
        
        @Override
        public String toString() {
            return String.format("card %s at %s with vendors %s", cardNumber, new Date(windowTime), vendorIds);
        }
    }
    
    @SuppressWarnings("serial")
    private static class CountFraudPerVendorWindow implements AggregateFunction<Tuple2<String, Long>, Integer, Integer> {

        @Override
        public Integer add(Tuple2<String, Long> in, Integer acc) {
            return acc + 1;
        }

        @Override
        public Integer createAccumulator() {
            return 0;
        }

        @Override
        public Integer getResult(Integer acc) {
            return acc;
        }

        @Override
        public Integer merge(Integer acc1, Integer acc2) {
            return acc1 + acc2;
        }
    }
    
    @SuppressWarnings("serial")
    private static class AggregateUniqueCards implements AggregateFunction<TransactionRecord, Set<String>, VendorsPerCardWindow> {

        @Override
        public Set<String> add(TransactionRecord in, Set<String> acc) {
            System.out.println("AggregateUniqueCards: " + in);
            acc.add(in.getVendorId());
            return acc;
        }

        @Override
        public Set<String> createAccumulator() {
            return new HashSet<>();
        }

        @Override
        public VendorsPerCardWindow getResult(Set<String> acc) {
            VendorsPerCardWindow result = new VendorsPerCardWindow();
            result.setVendorIds(new ArrayList<>(acc));
            return result;
        }

        @Override
        public Set<String> merge(Set<String> acc1, Set<String> acc2) {
            acc1.addAll(acc2);
            return acc1;
        }
    }
    
    @SuppressWarnings("serial")
    private static class CreateVendorsPerCardWindow extends ProcessWindowFunction<VendorsPerCardWindow, VendorsPerCardWindow, String, TimeWindow> {

        private static final Logger LOGGER = LoggerFactory.getLogger(CreateVendorsPerCardWindow.class)
                ;
        @Override
        public void process(String cardNumber, Context ctx,
                Iterable<VendorsPerCardWindow> iter, Collector<VendorsPerCardWindow> out) throws Exception {
            VendorsPerCardWindow result = iter.iterator().next();
            result.setCardNumber(cardNumber);
            result.setWindowTime(ctx.window().getEnd());
            
            System.out.format("Generating vendors per card per window: %s\n", result);
            out.collect(result);
        }


    }

        
}
