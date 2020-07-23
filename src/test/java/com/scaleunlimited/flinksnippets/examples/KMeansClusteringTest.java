package com.scaleunlimited.flinksnippets.examples;

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.scaleunlimited.flinksnippets.examples.KMeansClustering.Centroid;
import com.scaleunlimited.flinksnippets.examples.KMeansClustering.CentroidType;
import com.scaleunlimited.flinksnippets.examples.KMeansClustering.Feature;

public class KMeansClusteringTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(KMeansClusteringTest.class);

    @Test
    public void test() throws Exception {
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.createLocalEnvironment(2);

        String[] centers = KMeansData.INITIAL_CENTERS_2D.split("\n");
        DataStream<Centroid> centroidsSource = env.fromElements(centers)
                .map(new MapFunction<String, Centroid>() {
                    private static final long serialVersionUID = 1L;

                    public Centroid map(String c) {
                        String[] fields = c.split("\\|");
                        try {
                            Feature f = new Feature(Double.parseDouble(fields[1]),
                                    Double.parseDouble(fields[2]));
                            return new Centroid(f, Integer.parseInt(fields[0]), CentroidType.VALUE);
                        } catch (Exception e) {
                            LOGGER.error("Failure parsing centroid data: " + c, e);
                            throw new RuntimeException(e);
                        }
                    }
                });

        String[] points = KMeansData.DATAPOINTS_2D.split("\n");
        DataStream<Feature> featuresSource = env.fromElements(points)
                .map(new MapFunction<String, Feature>() {
                    private static final long serialVersionUID = 1L;

                    public Feature map(String p) {
                        String[] fields = p.split("\\|");
                        try {
                            return new Feature(
                                    Double.parseDouble(fields[1]),
                                    Double.parseDouble(fields[2]));
                        } catch (Exception e) {
                            LOGGER.error("Failure parsing feature data: " + p, e);
                            throw new RuntimeException(e);
                        }
                    }
                })
                .map(new ClusterizePoints(centers));

        KMeansClustering.build(env, centroidsSource, featuresSource);
        env.execute();
    }

    @Ignore
    @Test
    public void testSyntheticData() throws Exception {
        final StreamExecutionEnvironment env = 
                StreamExecutionEnvironment.createLocalEnvironment(2);

        final int numCentroids = 2;
        Centroid[] centroids = makeCentroids(numCentroids);
        DataStream<Centroid> centroidsSource = env.fromElements(Centroid.class, centroids);

        final int numPoints = numCentroids * 100;
        DataStream<Feature> featuresSource = env.fromElements(Feature.class,
                makeFeatures(centroids, numPoints));

        KMeansClustering.build(env, centroidsSource, featuresSource);

        env.execute();
    }

    private static Centroid[] makeCentroids(int numCentroids) {
        Centroid[] result = new Centroid[numCentroids];
        for (int i = 0; i < numCentroids; i++) {
            int x = ((i + 1) * 10);
            int y = ((i + 1) * 20);
            result[i] = new Centroid(new Feature(x, y), i, CentroidType.VALUE);
        }

        return result;
    }

    private static Feature[] makeFeatures(Centroid[] centroids, int numFeatures) {
        int numCentroids = centroids.length;
        Random rand = new Random(0L);
        Feature[] result = new Feature[numFeatures];
        for (int i = 0; i < numFeatures; i++) {
            double x = rand.nextDouble() * 10 * numCentroids;
            double y = rand.nextDouble() * 20 * numCentroids;
            result[i] = new Feature(x, y);
        }

        return result;
    }

    @SuppressWarnings("serial")
    private static class ClusterizePoints extends RichMapFunction<Feature, Feature> {
        
        private String[] clusters;
        private transient Map<Integer, Centroid> centroids;
        
        public ClusterizePoints(String[] clusters) {
            this.clusters = clusters;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            
            centroids = new HashMap<>();
            
            for (String p : clusters) {
                String[] fields = p.split("\\|");
                Feature f = new Feature(Double.parseDouble(fields[1]),
                        Double.parseDouble(fields[2]));
                Centroid c = new Centroid(f, Integer.parseInt(fields[0]), CentroidType.VALUE);
                centroids.put(c.getId(), c);
                LOGGER.debug(String.format("Adding cluster %d: %s", c.getId(), f));
            }

        }
        
        @Override
        public Feature map(Feature value) throws Exception {
            double minDistance = Double.MAX_VALUE;
            Centroid bestCentroid = null;
            for (Centroid centroid : centroids.values()) {
                double distance = centroid.distance(value);
                if (distance < minDistance) {
                    minDistance = distance;
                    bestCentroid = centroid;
                }
            }

            // Move the point part of the way to the best centroid cluster
            double newX = value.getX() - (value.getX() - bestCentroid.getFeature().getX()) * 0.3;
            double newY = value.getY() - (value.getY() - bestCentroid.getFeature().getY()) * 0.3;
            
            LOGGER.debug(String.format("Moving towards cluster %d (%f,%f): %f,%f => %f,%f", 
                    bestCentroid.getId(), bestCentroid.getFeature().getX(), bestCentroid.getFeature().getY(),
                    value.getX(), value.getY(), newX, newY));
            return new Feature(newX, newY);
        }
        
        
    }
}
