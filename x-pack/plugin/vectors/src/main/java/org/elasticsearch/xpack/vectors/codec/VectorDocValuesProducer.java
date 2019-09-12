package org.elasticsearch.xpack.vectors.codec;

import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.util.IntArray;

import java.io.IOException;
import java.nio.ByteBuffer;


public class VectorDocValuesProducer extends DocValuesProducer {
    private final IntArray documentCentroids;
    private final BytesRef[] centroids;
    private final BinaryDocValues delegate;

    VectorDocValuesProducer(IntArray documentCentroids,
                            float[][] centroids,
                            BinaryDocValues delegate) {
        this.documentCentroids = documentCentroids;
        this.delegate = delegate;

        this.centroids = new BytesRef[centroids.length];
        for (int c = 0; c < centroids.length; c++) {
            byte[] bytes = new byte[4 * centroids[c].length + 4];
            ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
            float norm = 0.0f;

            for (float value : centroids[c]) {
                byteBuffer.putFloat(value);
                norm += value * value;
            }

            byteBuffer.putFloat((float)Math.sqrt(norm));
            this.centroids[c] = new BytesRef(bytes);
        }
    }

    @Override
    public NumericDocValues getNumeric(FieldInfo field) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public BinaryDocValues getBinary(FieldInfo field) throws IOException {
        return new BinaryDocValues() {

            @Override
            public BytesRef binaryValue() throws IOException {
                int c = documentCentroids.get(docID());
                return centroids[c];
            }

            @Override
            public boolean advanceExact(int target) throws IOException {
                return delegate.advanceExact(target);
            }

            @Override
            public int docID() {
                return delegate.docID();
            }

            @Override
            public int nextDoc() throws IOException {
                return delegate.nextDoc();
            }

            @Override
            public int advance(int target) throws IOException {
                return delegate.advance(target);
            }

            @Override
            public long cost() {
                return delegate.cost();
            }
        };
    }

    @Override
    public SortedDocValues getSorted(FieldInfo field) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public SortedNumericDocValues getSortedNumeric(FieldInfo field) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public SortedSetDocValues getSortedSet(FieldInfo field) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void checkIntegrity() throws IOException {}

    @Override
    public void close() throws IOException {}

    @Override
    public long ramBytesUsed() {
        return 0;
    }
}

