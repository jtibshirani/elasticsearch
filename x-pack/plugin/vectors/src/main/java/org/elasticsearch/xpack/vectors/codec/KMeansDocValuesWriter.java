package org.elasticsearch.xpack.vectors.codec;

import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Version;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.IntArray;
import org.elasticsearch.xpack.vectors.mapper.VectorEncoderDecoder;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;

public class KMeansDocValuesWriter extends DocValuesConsumer {
    private static final int NUM_CENTROIDS = 1000;
    private static final int NUM_ITERS = 2;

    private final SegmentWriteState state;
    private final DocValuesConsumer delegate;

    private final Random random;
    private final BigArrays bigArrays;

    public KMeansDocValuesWriter(SegmentWriteState state,
                                 DocValuesConsumer delegate) {
        this.state = state;
        this.delegate = delegate;

        this.random = new Random(42L);
        this.bigArrays = BigArrays.NON_RECYCLING_INSTANCE;
    }

    @Override
    public void addBinaryField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
        if (field.name.equals("vector") == false) {
            return;
        }

        System.out.println("Running k-means...");

        BinaryDocValues values = valuesProducer.getBinary(field);
        float[][] centroids = new float[NUM_CENTROIDS][];

        int numDocs = 0;
        while (values.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
            BytesRef bytes = values.binaryValue();

            if (numDocs < NUM_CENTROIDS) {
                centroids[numDocs] = decodeVector(bytes);
            } else if (random.nextDouble() < NUM_CENTROIDS * (1.0 / numDocs)) {
                int c = random.nextInt(NUM_CENTROIDS);
                centroids[c] = decodeVector(bytes);
            }
            numDocs++;
        }

        IntArray documentCentroids = bigArrays.newIntArray(state.segmentInfo.maxDoc());
        for (int iter = 0; iter < NUM_ITERS; iter++) {
            centroids = runIter(iter, field, valuesProducer, centroids, documentCentroids);
        }

        System.out.println("Finished k-means on [" + numDocs + "] docs.");
        System.out.println("Writing quantized vectors...");

        FieldInfo quantizedField = state.fieldInfos.fieldInfo("vector-quantized");
        VectorDocValuesProducer quantizedProducer = new VectorDocValuesProducer(
            documentCentroids,
            centroids,
            valuesProducer.getBinary(field));
        delegate.addBinaryField(quantizedField, quantizedProducer);

        System.out.println("Finished writing quantized vectors.");
        System.out.println("Writing original vectors...");

        delegate.addBinaryField(field, valuesProducer);

        System.out.println("Finished writing original vectors.");
    }

    /**
     * Runs one iteration of k-means. For each document vector, we first find the
     * nearest centroid, then update the location of the new centroid.
     */
    private float[][] runIter(int iter,
                              FieldInfo field,
                              DocValuesProducer valuesProducer,
                              float[][] centroids,
                              IntArray documentCentroids) throws IOException {
        double distToBestCentroid = 0.0;
        double distToOtherCentroids = 0.0;
        int numDocs = 0;

        float[][] newCentroids = new float[centroids.length][centroids[0].length];
        int[] newCentroidSize = new int[centroids.length];

        BinaryDocValues values = valuesProducer.getBinary(field);
        while (values.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
            BytesRef bytes = values.binaryValue();
            float[] vector = decodeVector(bytes);

            int bestCentroid = -1;
            double bestDist = Double.MAX_VALUE;
            for (int c = 0; c < centroids.length; c++) {
                double dist = l2norm(centroids[c], vector);
                distToOtherCentroids += dist;

                if (dist < bestDist) {
                    bestCentroid = c;
                    bestDist = dist;
                }
            }

            documentCentroids.set(values.docID(), bestCentroid);
            for (int v = 0; v < vector.length; v++) {
                newCentroids[bestCentroid][v] += vector[v];
                newCentroidSize[bestCentroid]++;
            }

            distToBestCentroid += bestDist;
            distToOtherCentroids -= bestDist;
            numDocs++;
        }

        for (int c = 0; c < newCentroids.length; c++) {
            for (int v = 0; v < centroids[c].length; v++) {
                newCentroids[c][v] /= newCentroidSize[c];
            }
        }

        distToBestCentroid /= numDocs;
        distToOtherCentroids /= numDocs * (NUM_CENTROIDS - 1);

        System.out.println("Finished iteration [" + iter + "]. Dist to centroid [" + distToBestCentroid +
            "], dist to other centroids [" + distToOtherCentroids + "].");
        return newCentroids;
    }

    private float[] decodeVector(BytesRef bytes) {
        int vectorLength = VectorEncoderDecoder.denseVectorLength(Version.V_7_5_0, bytes);
        float[] vector = new float[vectorLength];

        ByteBuffer byteBuffer = ByteBuffer.wrap(bytes.bytes, bytes.offset, bytes.length);
        for (int v = 0; v < vectorLength; v++) {
            vector[v] = byteBuffer.getFloat();
        }
        return vector;
    }
    
    private double l2norm(float[] first, float[] second) {
        double l2norm = 0;
        for (int v = 0; v < first.length; v++) {
            double diff = first[v] - second[v];
            l2norm += diff * diff;
        }
        return Math.sqrt(l2norm);
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }

    @Override
    public void addNumericField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
        delegate.addNumericField(field, valuesProducer);
    }

    @Override
    public void addSortedField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
        delegate.addSortedField(field, valuesProducer);
    }

    @Override
    public void addSortedNumericField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
        delegate.addSortedNumericField(field, valuesProducer);
    }

    @Override
    public void addSortedSetField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
        delegate.addSortedSetField(field, valuesProducer);
    }
}
