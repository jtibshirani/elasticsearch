package org.elasticsearch.xpack.vectors.mapper;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class RandomProjector {
    private static final int NUM_DIMENSIONS = 8;

    private final List<float[][]> bases;

    private RandomProjector(List<float[][]> bases) {
        this.bases = bases;
    }

    public static RandomProjector create(int vectorDimension,
                                         int numProjections) {
        Random random = new Random();
        List<float[][]> bases = new ArrayList<>(numProjections);

        for (int p = 0; p < numProjections; p++) {
            float[][] basis = new float[NUM_DIMENSIONS][vectorDimension];
            for (int d = 0; d < NUM_DIMENSIONS; d++) {
                basis[d] = randomGaussian(random, vectorDimension);
            }
            bases.add(basis);
        }
        return new RandomProjector(bases);
    }

    private static float[] randomGaussian(Random random, int vectorDimension) {
        float[] result = new float[vectorDimension];
        for (int i = 0; i < vectorDimension; i++) {
            result[i] = (float) random.nextGaussian();
        }
        return result;
    }

    private static float[] randomBernouilli(Random random, int vectorDimension) {
        float[] result = new float[vectorDimension];
        for (int i = 0; i < vectorDimension; i++) {
            result[i] = random.nextBoolean() ? -1 : 1;
        }
        return result;
    }

    List<float[]> project(float[] vector) {
        List<float[]> points = new ArrayList<>(bases.size());
        for (float[][] basis : bases) {
            float[] point = project(vector, basis);
            points.add(point);
        }
        return points;
    }

    private static float[] project(float[] vector, float[][] basis) {
        float[] result = new float[NUM_DIMENSIONS];
        for (int i = 0; i < basis.length; i++) {
            result[i] = dotProduct(vector, basis[i]);
        }
        return result;
    }

    private static float dotProduct(float[] vector, float[] projection) {
        float result = 0.0f;
        for (int i = 0; i < vector.length; i++) {
            result += vector[i] * projection[i];
        }
        return result;
    }
}
