package com.scaleunlimited.flinksnippets.examples;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Cluster points via K-Means clustering. The algorithm is:
 *   - Start with N random clusters (centroids)
 *   - Feed points (unassigned to cluster) to the ClusterFunction
 *      - Assign each point to the nearest cluster
 *      - If the nearest cluster is different, iterate on the point
 *      - Recalculate cluster centroids
 *      - If a cluster centroid changes, iterate on the cluster (broadcast)
 *
 * We'll iterate until no points are changing clusters.
 *
 */
public class KMeansClustering {
    private static final Logger LOGGER = LoggerFactory.getLogger(KMeansClustering.class);

    public static void build(StreamExecutionEnvironment env, DataStream<Centroid> centroidsSource, DataStream<Feature> featuresSource) {
        IterativeStream<Centroid> centroidsIter = centroidsSource.iterate(5000L);
        IterativeStream<Feature> featuresIter = featuresSource.iterate(5000L);
        
        SplitStream<Tuple3<Feature,Centroid,Centroid>> comboStream = centroidsIter.broadcast()
            .connect(featuresIter.shuffle())
            .flatMap(new ClusterFunction())
            .split(new KmeansSelector());
        
        centroidsIter.closeWith(comboStream.select(KmeansSelector.CENTROID_UPDATE.get(0))
                .map(new MapFunction<Tuple3<Feature,Centroid,Centroid>, Centroid>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Centroid map(Tuple3<Feature, Centroid, Centroid> value) throws Exception {
                        return value.f1;
                    }
                }));
        
        featuresIter.closeWith(comboStream.select(KmeansSelector.FEATURE.get(0))
                .map(new MapFunction<Tuple3<Feature,Centroid,Centroid>, Feature>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Feature map(Tuple3<Feature, Centroid, Centroid> value) throws Exception {
                        return value.f0;
                    }
                }));
        
        comboStream.select(KmeansSelector.CENTROID_OUTPUT.get(0))
        .map(new MapFunction<Tuple3<Feature,Centroid,Centroid>, Centroid>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Centroid map(Tuple3<Feature, Centroid, Centroid> value) throws Exception {
                return value.f2;
            }
        })
        .print();
    }
    
    @SuppressWarnings("serial")
    private static class KmeansSelector implements OutputSelector<Tuple3<Feature,Centroid,Centroid>> {

        public static final List<String> FEATURE = Arrays.asList("feature");
        public static final List<String> CENTROID_UPDATE = Arrays.asList("centroid-update");
        public static final List<String> CENTROID_OUTPUT = Arrays.asList("centroid-output");

        @Override
        public Iterable<String> select(Tuple3<Feature, Centroid, Centroid> value) {
            if (value.f0 != null) {
                return FEATURE;
            } else if (value.f1 != null) {
                return CENTROID_UPDATE;
            } else if (value.f2 != null) {
                return CENTROID_OUTPUT;
            } else {
                throw new RuntimeException("Invalid case of all fields null");
            }
        }
    }
    
    public static class Feature {
        private long time;
        private int processCount;
        private double x;
        private double y;
        private int centroidId;

        public Feature() {
            this(0, 0);
        }

        public Feature(double x, double y) {
            this(x, y, -1);
        }

        public Feature(double x, double y, int centroidId) {
            this.time = System.currentTimeMillis();
            this.x = x;
            this.y = y;
            this.centroidId = centroidId;
            this.processCount = 0;
        }

        public Feature(Feature p) {
            this(p.getX(), p.getY(), p.getCentroidId());
            this.time = p.getTime();
        }

        public int getCentroidId() {
            return centroidId;
        }

        public Feature setCentroidId(int centroidId) {
            this.centroidId = centroidId;
            return this;
        }
        
        public long getTime() {
            return time;
        }
        
        public Feature setTime(long time) {
            this.time = time;
            return this;
        }
        
        public int getProcessCount() {
            return processCount;
        }
        
        public void incProcessCount() {
            processCount += 1;
        }
        
        public void setProcessCount(int processCount) {
            this.processCount = processCount;
        }
        
        public double distance(Feature feature) {
            return Math.sqrt(Math.pow(feature.x - x, 2) + Math.pow(feature.y - y, 2));
        }
        
        public Feature times(int scalar) {
            x *= scalar;
            y *= scalar;
            return this;
        }
        
        public Feature divide(int scalar) {
            x /= scalar;
            y /= scalar;
            return this;
        }
        
        public Feature plus(Feature feature) {
            x += feature.x;
            y += feature.y;
            return this;
        }
        
        public Feature minus(Feature feature) {
            x -= feature.x;
            y -= feature.y;
            return this;
        }
        
        public double getX() {
            return x;
        }
        
        public double getY() {
            return y;
        }
        
        @Override
        public String toString() {
            if (centroidId != -1) {
                return String.format("%f,%f (%s)", x, y, centroidId);
            } else {
                return String.format("%f,%f", x, y);
            }
        }
    }

    public static enum CentroidType {
        VALUE,
        ADD,
        REMOVE
    }
    
    public static class Centroid {
        private CentroidType type;
        private long time;
        private Feature feature;
        private int id;
        private int numFeatures;
        
        public Centroid() {
        }

        public Centroid(Feature f, int centroidId, CentroidType type) {
            this.time = System.currentTimeMillis();
            this.type = type;
            this.feature = f;
            this.id = centroidId;
            this.numFeatures = 1;
        }

        public int getId() {
            return id;
        }
        
        public CentroidType getType() {
            return type;
        }
        
        public double distance(Feature f) {
            return feature.distance(f);
        }
        
        public void addFeature(Feature f) {
            feature.times(numFeatures).plus(f).divide(numFeatures + 1);
            numFeatures += 1;
        }

        public void removeFeature(Feature f) {
            if (numFeatures > 1) {
                feature.times(numFeatures).minus(f).divide(numFeatures - 1);
                numFeatures -= 1;
            } else {
                throw new RuntimeException(String.format("Centroid %d can't have 0 features!", id));
            }
        }

        public Feature getFeature() {
            return feature;
        }
        
        public long getTime() {
            return time;
        }
        
        public void setTime(long time) {
            this.time = time;
        }
        
        @Override
        public String toString() {
            return String.format("%d (%s) %s with %d features", id, type, feature.toString(), numFeatures);
        }
    }

    @SuppressWarnings("serial")
    private static class ClusterFunction extends RichCoFlatMapFunction<Centroid, Feature, Tuple3<Feature, Centroid, Centroid>> {

        private static int MAX_QEUEUED_FEATURES = 1000;
        
        private transient Map<Integer, Centroid> centroids;
        private transient Queue<Feature> featureQueue;
        private transient long centroidTimestamp;
        
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);

            centroids = new HashMap<>();
            featureQueue = new LinkedList<>();
            centroidTimestamp = 0;
        }

        @Override
        public void flatMap1(Centroid centroid, Collector<Tuple3<Feature, Centroid, Centroid>> out) throws Exception {
            LOGGER.debug("Centroid {} @ {} >> Updating", centroid, centroid.getTime());
            
            centroidTimestamp = centroid.getTime() - 100L;
            
            // Process queued features that are earlier than centroidTimestamp
            while (!featureQueue.isEmpty()) {
                Feature feature = featureQueue.peek();
                if (feature.getTime() < centroidTimestamp) {
                    processFeature(featureQueue.remove(), out);
                }
            }
            
            int id = centroid.getId();
            switch (centroid.getType()) {
                case VALUE:
                    if (centroids.containsKey(id)) {
                        throw new RuntimeException(String.format("Got initial centroid %s but it already exists!", centroid));
                    }
                    
                    centroids.put(id, centroid);
                break;
                
                case ADD:
                    if (!centroids.containsKey(id)) {
                        throw new RuntimeException(String.format("Got centroid add %s but it doesn't exist!", centroid));
                    }
                
                    centroids.get(id).addFeature(centroid.getFeature());
                    break;
                
                case REMOVE:
                    if (!centroids.containsKey(id)) {
                        throw new RuntimeException(String.format("Got centroid remove %s but it doesn't exist!", centroid));
                    }
                
                    centroids.get(id).removeFeature(centroid.getFeature());
                    break;
                
                default:
                    throw new RuntimeException(String.format("Got unknown centroid type %s!", centroid.getType()));
                
            }
            
            out.collect(new Tuple3<>(null, null, centroids.get(id)));
        }

        @Override
        public void flatMap2(Feature feature, Collector<Tuple3<Feature, Centroid, Centroid>> out) throws Exception {
            if (feature.getTime() >= centroidTimestamp) {
                while (featureQueue.size() >= MAX_QEUEUED_FEATURES) {
                    processFeature(featureQueue.remove(), out);
                }
                
                LOGGER.debug("Feature {} @ {} >> queueing", feature, feature.getTime());
                featureQueue.add(feature);
                return;
            } else {
                processFeature(feature, out);
            }
        }

        private void processFeature(Feature feature, Collector<Tuple3<Feature, Centroid, Centroid>> out) {
            double minDistance = Double.MAX_VALUE;
            Centroid bestCentroid = null;
            for (Centroid centroid : centroids.values()) {
                double distance = centroid.distance(feature);
                if (distance < minDistance) {
                    minDistance = distance;
                    bestCentroid = centroid;
                }
            }

            if (bestCentroid == null) {
                LOGGER.debug("Feature {} @ {} >> No centroids yet, recycling", feature, feature.getTime());
                feature.setTime(System.currentTimeMillis());
                out.collect(new Tuple3<>(feature, null, null));
                return;
            }

            int oldCentroidId = feature.getCentroidId();
            int newCentroidId = bestCentroid.getId();
            
            // We'll add the value to the cluster, to continue influencing its
            // centroid.
            Centroid add = new Centroid(feature, newCentroidId, CentroidType.ADD);
            out.collect(new Tuple3<>(null, add, null));

            if (oldCentroidId == newCentroidId) {
                feature.incProcessCount();
                if (feature.getProcessCount() >= 20) {
                    LOGGER.debug("Feature {} @ {} >> stable, removing", feature, feature.getTime());
                } else {
                    LOGGER.debug("Feature {} @ {} >> not stable enough, recycling", feature, feature.getTime());
                    feature.setTime(System.currentTimeMillis());
                    out.collect(new Tuple3<>(feature, null, null));
                }
            } else {
                feature.setProcessCount(1);
                
                if (oldCentroidId != -1) {
                    Centroid removal = new Centroid(feature.times(feature.getProcessCount()), oldCentroidId, CentroidType.REMOVE);
                    out.collect(new Tuple3<>(null, removal, null));
                }

                LOGGER.debug("Feature {} @ {} >> moving to {}", feature, feature.getTime(), bestCentroid.getId());
                feature.setCentroidId(newCentroidId);
                feature.setTime(System.currentTimeMillis());
                out.collect(new Tuple3<>(feature, null, null));
            }
        }

    }
}
