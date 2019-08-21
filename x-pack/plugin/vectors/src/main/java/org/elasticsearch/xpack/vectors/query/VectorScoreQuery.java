package org.elasticsearch.xpack.vectors.query;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Version;
import org.elasticsearch.xpack.vectors.mapper.VectorEncoderDecoder;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;
import java.util.Set;

public class VectorScoreQuery extends Query {
    public enum Metric {COSINE, L2 }

    private final String field;
    private final Metric metric;
    private final float[] queryVector;

    VectorScoreQuery(String field, Metric metric, float[] queryVector) {
        this.field = field;
        this.metric = metric;

        this.queryVector = queryVector;
        if (metric == Metric.COSINE) {
            double queryNorm = Math.sqrt(dotProduct(queryVector, queryVector));
            for (int i = 0; i < queryVector.length; i++) {
                this.queryVector[i] /= queryNorm;
            }
        }
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
        return new Weight(this) {
            @Override
            public void extractTerms(Set<Term> terms) {
                throw new UnsupportedOperationException();
            }

            @Override
            public Explanation explain(LeafReaderContext context, int doc) throws IOException {
                throw new UnsupportedOperationException();
            }

            @Override
            public Scorer scorer(LeafReaderContext context) throws IOException {
                if (metric == Metric.COSINE) {
                    return new CosineScorer(this, field, queryVector, context);
                } else if (metric == Metric.L2) {
                    return new L2Scorer(this, field, queryVector, context);
                } else {
                    throw new UnsupportedOperationException();
                }
            }

            @Override
            public boolean isCacheable(LeafReaderContext ctx) {
                return false;
            }
        };
    }

    private static class CosineScorer extends Scorer {
        private final float[] queryVector;
        private final BinaryDocValues docValues;

        CosineScorer(Weight weight,
                     String field,
                     float[] queryVector,
                     LeafReaderContext context) throws IOException {
            super(weight);
            this.queryVector = queryVector;
            this.docValues = context.reader().getBinaryDocValues(field);
        }

        @Override
        public DocIdSetIterator iterator() {
            return docValues;
        }

        @Override
        public float getMaxScore(int upTo) {
            return Float.MAX_VALUE;
        }

        @Override
        public float score() throws IOException {
            BytesRef vector = docValues.binaryValue();
            ByteBuffer byteBuffer = ByteBuffer.wrap(vector.bytes, vector.offset, vector.length);

            double dotProduct = 0.0;
            for (float queryValue : queryVector) {
                dotProduct += queryValue * byteBuffer.getFloat();
            }
            double docNorm = VectorEncoderDecoder.decodeVectorMagnitude(Version.CURRENT, vector);

            return (float) (dotProduct / docNorm + 1.0);
        }

        @Override
        public int docID() {
            return docValues.docID();
        }
    }

    private class L2Scorer extends Scorer {
        private final float[] queryVector;
        private final BinaryDocValues docValues;

        L2Scorer(Weight weight,
                 String field,
                 float[] queryVector,
                 LeafReaderContext context) throws IOException {
            super(weight);
            this.queryVector = queryVector;
            this.docValues = context.reader().getBinaryDocValues(field);
        }

        @Override
        public DocIdSetIterator iterator() {
            return docValues;
        }

        @Override
        public float getMaxScore(int upTo) {
            return Float.MAX_VALUE;
        }

        @Override
        public float score() throws IOException {
            BytesRef vector = docValues.binaryValue();
            ByteBuffer byteBuffer = ByteBuffer.wrap(vector.bytes, vector.offset, vector.length);

            double result = 0;
            for (float queryValue : queryVector) {
                float diff = queryValue - byteBuffer.getFloat();
                result += diff * diff;
            }

            return (float) (1.0 / (1.0 + Math.sqrt(result)));
        }

        @Override
        public int docID() {
            return docValues.docID();
        }
    }

    private static double dotProduct(float[] a, float[] b){
        double result = 0;
        for (int dim = 0; dim < a.length; dim++) {
            result += a[dim] * b[dim];
        }
        return result;
    }


    @Override
    public String toString(String field) {
        return new StringBuilder()
            .append("VectorScoreQuery(field=")
            .append(field)
            .append(", metric=")
            .append(metric)
            .append(", queryVector=")
            .append(Arrays.toString(queryVector))
            .append(")")
            .toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        VectorScoreQuery that = (VectorScoreQuery) o;
        return Objects.equals(field, that.field) &&
            Arrays.equals(queryVector, that.queryVector);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(field);
        result = 31 * result + Arrays.hashCode(queryVector);
        return result;
    }
}
