package org.elasticsearch.xpack.vectors.query;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Version;
import org.elasticsearch.script.ScoreScript;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptEngine;
import org.elasticsearch.search.lookup.LeafDocLookup;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.xpack.vectors.mapper.VectorEncoderDecoder;

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

        private final List<Number> queryVector;
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

            queryVector = (List<Number>) params.get("query_vector");
        }

        @Override
        public boolean needs_score() {
            return false;
        }

        @Override
        public ScoreScript newInstance(LeafReaderContext context) {
            ScoreScript script = new ScoreScript(params, lookup, context) {
                @Override
                public double execute() {
                    return 0;
                }
            };
            script._setIndexVersion(Version.CURRENT);

            ScoreScriptUtils.CosineSimilarity cosine = new ScoreScriptUtils.CosineSimilarity(script, queryVector);
            LeafDocLookup docLookup = lookup.doc().getLeafDocLookup(context);

            return new ScoreScript(params, lookup, context) {

                @Override
                public void setDocument(int docId) {
                    docLookup.setDocument(docId);
                }

                @Override
                public double execute() {
                    VectorScriptDocValues.DenseVectorScriptDocValues docValues = (VectorScriptDocValues.DenseVectorScriptDocValues) docLookup.get(field);
                    return cosine.cosineSimilarity(docValues) + 1.0;
                }
            };
        }
    }
}
