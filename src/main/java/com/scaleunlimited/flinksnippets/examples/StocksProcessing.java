package com.scaleunlimited.flinksnippets.examples;

import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;

public class StocksProcessing {
    
    public static class StockPrice {
        private String symbol;
        private double price;
        
        public StockPrice() {
            symbol = "";
            price = 0.0;
        }
        
        public StockPrice(String symbol, double price) {
            this.symbol = symbol;
            this.price = price;
        }

        public String getSymbol() {
            return symbol;
        }

        public double getPrice() {
            return price;
        }

        public void setSymbol(String symbol) {
            this.symbol = symbol;
        }

        public void setPrice(double price) {
            this.price = price;
        }
        
        @Override
        public String toString() {
            return String.format("%s: %02f", symbol, price);
        }
    }
    
    public static void main(String[] args) throws Exception {
            final StreamExecutionEnvironment env =
                    StreamExecutionEnvironment.getExecutionEnvironment();
        
                //Read from a socket stream at map it to StockPrice objects
            env.setParallelism(1);
                DataStream<StockPrice> stockStream = env.fromElements(StockPrice.class, makePrices(10));
                
                AllWindowedStream<StockPrice, GlobalWindow> windowedStream = stockStream
                        .countWindowAll(5, 2);
                        
                        //.keyBy("symbol")
                        //.timeWindowAll(Time.of(10, TimeUnit.SECONDS), Time.of(1, TimeUnit.SECONDS));
                
                    //stockStream.keyBy("symbol");
                    //Compute some simple statistics on a rolling window
                    DataStream<StockPrice> lowest = windowedStream.minBy("price");
                    //DataStream<StockPrice> highest = windowedStream.;
                    /*DataStream<StockPrice> maxByStock = windowedStream.groupBy("symbol")
                        .maxBy("price").flatten();
                    DataStream<StockPrice> rollingMean = windowedStream.groupBy("symbol")
                        .mapWindow(new WindowMean()).flatten();*/
                    lowest.print();
                    
                /*    
                    AllWindowedStream<StockPrice, GlobalWindow> windowedStream1 = lowest
                            .countWindowAll(5,2);
                    //windowedStream1.print();
                    DataStream<StockPrice> highest = windowedStream1.minBy("price");*/
                    //highest.print();
                    
                    env.execute("Stock stream");
        }

    private static StockPrice[] makePrices(int numPrices) {
        StockPrice[] result = new StockPrice[numPrices];
        for (int i = 0; i < numPrices; i++) {
            result[i] = new StockPrice("SPX", (double)(i + 1));
        }
        
        return result;
    }
}
