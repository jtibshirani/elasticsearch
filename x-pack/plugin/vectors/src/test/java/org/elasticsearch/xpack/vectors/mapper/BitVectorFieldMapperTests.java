/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */


package org.elasticsearch.xpack.vectors.mapper;

import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.DocumentMapperParser;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.elasticsearch.xpack.vectors.Vectors;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;

public class BitVectorFieldMapperTests extends ESSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(Vectors.class, LocalStateCompositeXPackPlugin.class);
    }

    // this allows to set indexVersion as it is a private setting
    @Override
    protected boolean forbidPrivateIndexSettings() {
        return false;
    }
    public void testDefaults() throws Exception {
        IndexService indexService = createIndex("test-index");
        DocumentMapperParser parser = indexService.mapperService().documentMapperParser();
        String mapping = Strings.toString(XContentFactory.jsonBuilder()
            .startObject()
            .startObject("_doc")
            .startObject("properties")
            .startObject("my-bit-vector").field("type", "bit_vector").field("dims", 3)
            .endObject()
            .endObject()
            .endObject()
            .endObject());
        DocumentMapper mapper = parser.parse("_doc", new CompressedXContent(mapping));

        int[] validVector = {1, 0, 1};

        ParsedDocument doc1 = mapper.parse(new SourceToParse("test-index", "1", BytesReference
            .bytes(XContentFactory.jsonBuilder()
                .startObject()
                    .startArray("my-bit-vector").value(validVector[0]).value(validVector[1]).value(validVector[2]).endArray()
                .endObject()),
            XContentType.JSON));
        IndexableField[] fields = doc1.rootDoc().getFields("my-bit-vector");
        assertEquals(1, fields.length);
        assertThat(fields[0], instanceOf(BinaryDocValuesField.class));
    }
}
