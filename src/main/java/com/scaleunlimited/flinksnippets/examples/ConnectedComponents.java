package com.scaleunlimited.flinksnippets.examples;

import java.util.Iterator;
import java.util.List;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.api.common.functions.JoinFunction;

import com.scaleunlimited.flinksnippets.examples.KMeansClustering.Centroid;


public class ConnectedComponents {

    private static final long MAX_TIMEOUT = 1000;

    public static void build(StreamExecutionEnvironment env, DataStream<Tuple3<Integer, List<Integer>, Integer>> neigborsList) {
        IterativeStream<Tuple3<Integer, List<Integer>, Integer>> beginning_loop = neigborsList.iterate(MAX_TIMEOUT);

        //Emits tuples Vertices and Labels for every vertex and its neighbors

        DataStream<Tuple2<Integer,Integer>> labels = beginning_loop
                //Datastream of <Vertex, label> for every neigborsList.f0 and element in neigborsList.f1
                .flatMap( new EmitVertexLabel() ) 
                .keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.milliseconds(windowSize)))
                .minBy(1)               
                ;


        DataStream<Tuple4<Integer, List<Integer>, Integer, Integer>> updatedVertex = beginning_loop                
                    //Update vertex label with the results from the labels reduction
                    .join(labels)
                    .where("vertex")
                    .equalTo("vertex")
                    .window(TumblingEventTimeWindows.of(Time.milliseconds(windowSize)))
                    .apply(new JoinFunction<Tuple3<Integer, List<Integer>, Integer>, Tuple2<Integer, Integer>, Tuple4<Integer, List<Integer>, Integer,Integer>>() {

                        @Override
                        public Tuple4<Integer, List<Integer>,Integer,Integer> join(
                                Tuple3<Integer, List<Integer>, Integer> arg0, Tuple2<Integer, Integer> arg1)
                                throws Exception {
                            int hasConverged = 1;
                            if (arg1.f1.intValue() < arg0.f0.intValue() ) {
                                arg0.f2 = arg1.f1;
                                hasConverged=0;
                            }
                            return new Tuple4<>(arg0.f0,arg0.f1,arg0.f2,new Integer(hasConverged));
                        }                       

                    })

                    //Disseminates the convergence flag if a change was made in the window
                    .windowAll(TumblingEventTimeWindows.of(Time.milliseconds(windowSize)))
                    .process(new ProcessAllWindowFunction<Tuple4<Integer,List<Integer>,Integer,Integer>,Tuple4<Integer, List<Integer>, Integer, Integer>,TimeWindow >() {

                        @Override
                        public void process(
                                ProcessAllWindowFunction<Tuple4<Integer, List<Integer>, Integer, Integer>, Tuple4<Integer, List<Integer>, Integer, Integer>, TimeWindow>.Context ctx,
                                Iterable<Tuple4<Integer, List<Integer>, Integer, Integer>> values,
                                Collector<Tuple4<Integer, List<Integer>, Integer, Integer>> out) throws Exception {

                            Iterator<Tuple4<Integer, List<Integer>, Integer, Integer>> iterator = values.iterator();
                            Tuple4<Integer, List<Integer>, Integer, Integer> element;

                            int hasConverged= 1;
                            while(iterator.hasNext())
                            {
                                element = iterator.next();
                                if(element.f3.intValue()>0)
                                {
                                    hasConverged=0;
                                    break;
                                }

                            }

                            //Re iterate and emit the values on the correct output
                            iterator = values.iterator();                           
                            Integer converged = new Integer(hasConverged);
                            while(iterator.hasNext())
                            {
                                element = iterator.next();
                                element.f3 = converged;
                                out.collect(element);

                            }                                                   
                        }
                    })              

                    ;


        DataStream<Tuple3<Integer, List<Integer>, Integer>> feed_back = updatedVertex
                .filter(new NotConvergedFilter())                               
                //Remove the finished convergence flag
                //Transforms the Tuples4 to Tuples3 so that it becomes compatible with beginning_loop
                .map(new RemoveConvergeceFlag())
                ;


        beginning_loop.closeWith(feed_back);

        //Selects the windows that have already converged
        DataStream<?> convergedWindows = updatedVertex
                .filter(new ConvergedFilter() );


        convergedWindows.print()
        .setParallelism(1)
        .name("Sink to stdout");

    }
}
