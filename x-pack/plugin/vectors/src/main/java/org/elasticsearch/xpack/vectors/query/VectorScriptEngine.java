package org.elasticsearch.xpack.vectors.query;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Version;
import org.elasticsearch.script.ScoreScript;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptEngine;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.xpack.vectors.mapper.VectorEncoderDecoder;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

public class VectorScriptEngine implements ScriptEngine {

    @Override
    public String getType() {
        return "vector_script";
    }

    @Override
    public <T> T compile(String scriptName, String scriptSource,
                         ScriptContext<T> context, Map<String, String> params) {
        if (context.equals(ScoreScript.CONTEXT) == false) {
            throw new IllegalArgumentException(getType()
                + " scripts cannot be used for context ["
                + context.name + "]");
        }

        ScoreScript.Factory factory = VectorScriptFactory::new;
        return context.factoryClazz.cast(factory);
    }

    @Override
    public void close() {
        // optionally close resources
    }

    private static class VectorScriptFactory implements ScoreScript.LeafFactory {
        private final Map<String, Object> params;
        private final SearchLookup lookup;

        private final float[] queryVector;
        private final String field;

        @SuppressWarnings("unchecked")
        private VectorScriptFactory(
            Map<String, Object> params, SearchLookup lookup) {
            if (params.containsKey("field") == false) {
                throw new IllegalArgumentException(
                    "Missing parameter [field]");
            }
            if (params.containsKey("query_vector") == false) {
                throw new IllegalArgumentException(
                    "Missing parameter [term]");
            }
            this.params = params;
            this.lookup = lookup;
            field = params.get("field").toString();

            List<Number> query = (List<Number>) params.get("query_vector");

            queryVector = new float[query.size()];
            double norm = 0.0;
            for (int i = 0; i < queryVector.length; i++) {
                float value = query.get(i).floatValue();
                queryVector[i] = query.get(i).floatValue();
                norm += value * value;
            }

            for (int i = 0; i < queryVector.length; i++) {
                queryVector[i] /= norm;
            }
        }

        @Override
        public boolean needs_score() {
            return false;
        }

        @Override
        public ScoreScript newInstance(LeafReaderContext context) {
            BinaryDocValues docValues;
            try {
                docValues = DocValues.getBinary(context.reader(), field);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }

            return new ScoreScript(params, lookup, context) {

                @Override
                public void setDocument(int docId) {
                    try {
                        docValues.advanceExact(docId);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                }

                @Override
                public double execute() {
                    BytesRef vector;
                    try {
                        vector = docValues.binaryValue();
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }

                    ByteBuffer byteBuffer = ByteBuffer.wrap(vector.bytes, vector.offset, vector.length);

                    double dotProduct = 0.0;
                    for (float queryValue : queryVector) {
                        dotProduct += queryValue * byteBuffer.getFloat();
                    }
                    double docNorm = VectorEncoderDecoder.decodeVectorMagnitude(Version.CURRENT, vector);

                    return (float) (dotProduct / docNorm + 1.0);
                }
            };
        }
    }
}
