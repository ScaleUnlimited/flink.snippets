package com.scaleunlimited.flinksnippets;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.junit.Test;

public class AsyncFunctionExampleTest {

    @Test
    public void test() throws Exception {
        LocalStreamEnvironment env = new LocalStreamEnvironment();
        env.setParallelism(2);

        DataStream<Integer> inputStream = env.fromElements(1, 2, 3, 4, 5);

        DataStream<Tuple2<Integer, String>> result = 
                AsyncDataStream.unorderedWait(inputStream, 
                        new AsyncDatabaseRequest(),
                        1000, TimeUnit.MILLISECONDS, 100);

        result.print();
        env.execute();
    }

    // This example implements the asynchronous request and callback with Futures that have the
    // interface of Java 8's futures (which is the same one followed by Flink's Future)

    /**
     * An implementation of the 'AsyncFunction' that sends requests and sets the callback.
     */
    @SuppressWarnings("serial")
    public static class AsyncDatabaseRequest extends RichAsyncFunction<Integer, Tuple2<Integer, String>> {

        /** The database specific client that can issue concurrent requests with callbacks */
        private transient DatabaseClient client;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            
            client = new DatabaseClient();
        }
        
        @Override
        public void close() throws Exception {
            client.close();
        }

        @Override
        public void asyncInvoke(Integer key, final ResultFuture<Tuple2<Integer, String>> resultFuture) throws Exception {

            // issue the asynchronous request, receive a future for result
            final Future<String> result = client.query(key);

            // set the callback to be executed once the request by the client is complete
            // the callback simply forwards the result to the result future
            CompletableFuture.supplyAsync(new Supplier<String>() {

                @Override
                public String get() {
                    try {
                        return result.get();
                    } catch (InterruptedException | ExecutionException e) {
                        // Normally handled explicitly.
                        return null;
                    }
                }
            }).thenAccept( (String dbResult) -> {
                resultFuture.complete(Collections.singleton(new Tuple2<>(key, dbResult)));
            });
        }
    }
    
    private static class DatabaseClient implements Closeable {

        @Override
        public void close() throws IOException { }

        public Future<String> query(Integer key) {
            return new Future<String>() {

                private boolean cancelled = false;
                private boolean done = false;
                
                @Override
                public boolean cancel(boolean mayInterruptIfRunning) {
                    if (done) {
                        return false;
                    } else {
                        cancelled = true;
                        return true;
                    }
                }

                @Override
                public boolean isCancelled() {
                    return cancelled;
                }

                @Override
                public boolean isDone() {
                    return done;
                }

                @Override
                public String get() throws InterruptedException, ExecutionException {
                    return String.format("value-%d", key);
                }

                @Override
                public String get(long timeout, TimeUnit unit)
                        throws InterruptedException, ExecutionException, TimeoutException {
                    return get();
                }
            };
        }
        
    }
}
 
 
