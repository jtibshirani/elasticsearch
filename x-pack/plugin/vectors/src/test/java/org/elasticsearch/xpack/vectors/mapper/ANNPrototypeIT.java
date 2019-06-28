package org.elasticsearch.xpack.vectors.mapper;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.vectors.Vectors;
import org.junit.Before;

import java.io.IOException;
import java.util.Collection;

import static org.elasticsearch.index.query.QueryBuilders.termsQuery;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.core.StringStartsWith.startsWith;

public class ANNPrototypeIT extends ESSingleNodeTestCase {
    private static final int DIMENSIONS = 8;

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(Vectors.class, XPackPlugin.class);
    }

    @Before
    public void setUpIndex() throws Exception {
        client().admin().indices().prepareCreate("test")
            .addMapping("_doc", XContentFactory.jsonBuilder()
            .startObject()
                .startObject("properties")
                    .startObject("vector")
                        .field("type", "dense_vector")
                        .field("dims", DIMENSIONS)
                    .endObject()
                .endObject()
            .endObject())
            .get();
        ensureGreen("test");
    }

    public void testANN() throws IOException {
        float[] queryVector = new float[DIMENSIONS];
        for (int i = 0; i < queryVector.length; i++) {
            queryVector[i] = 3.14f;
        }

        for (int doc = 0; doc < 20; doc++) {
            float[] vector = addNoise(queryVector);
            client().prepareIndex("test", "_doc", "noise_" + doc)
                .setSource(XContentFactory.jsonBuilder().startObject()
                    .array("vector", vector)
                    .endObject())
                .get();
        }

        for (int doc = 0; doc < 20; doc++) {
            float[] vector = addConstant(queryVector, 1.0f);
            client().prepareIndex("test", "_doc", "shift_" + doc)
                .setSource(XContentFactory.jsonBuilder().startObject()
                    .array("vector", vector)
                    .endObject())
                .get();
        }

        client().admin().indices()
            .prepareRefresh("test")
            .get();

        SearchResponse response = client().prepareSearch("test")
            .setQuery(termsQuery("vector", queryVector))
            .get();
        assertEquals(0, response.getFailedShards());

        assertEquals(10, response.getHits().getHits().length);
        for (SearchHit hit : response.getHits().getHits()) {
            assertThat(hit.getId(), startsWith("noise_"));
        }
    }

    private static float[] addNoise(float[] vector) {
        float[] result = new float[vector.length];
        for (int i = 0; i < vector.length; i++) {
            result[i] = vector[i] + (float) randomDoubleBetween(-0.10, 0.10, true);
        }

        return result;
    }

    private static float[] addConstant(float[] vector, float shift) {
        float[] result = new float[vector.length];
        for (int i = 0; i < vector.length; i++) {
            if (i % 2 == 0) {
                result[i] = vector[i] + shift;
            }
        }
        return result;
    }
}
