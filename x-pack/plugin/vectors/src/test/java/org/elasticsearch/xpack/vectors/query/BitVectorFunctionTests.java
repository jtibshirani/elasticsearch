/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.vectors.query;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Version;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.script.ScoreScript;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.vectors.query.ScoreScriptUtils.CosineSimilarity;
import org.elasticsearch.xpack.vectors.query.ScoreScriptUtils.DotProduct;
import org.elasticsearch.xpack.vectors.query.ScoreScriptUtils.L1Norm;
import org.elasticsearch.xpack.vectors.query.ScoreScriptUtils.L2Norm;
import org.elasticsearch.xpack.vectors.query.VectorScriptDocValues.DenseVectorScriptDocValues;
import org.junit.Before;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.elasticsearch.xpack.vectors.mapper.VectorEncoderDecoderTests.mockEncodeDenseVector;
import static org.hamcrest.Matchers.containsString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class BitVectorFunctionTests extends ESTestCase {
    private String field;
    private boolean[] docVector;
    private List<Boolean> queryVector;

    @Before
    public void setUpVectors() {
        field = "vector";
        docVector = new boolean[] {true, true, true, false, false};
        queryVector = Arrays.asList(true, true, true, false, true);
    }

    public void testHammingDistance() {
        BytesRef encodedDocVector = mockEncodeBitVector(docVector);
        DenseVectorScriptDocValues docValues = mock(DenseVectorScriptDocValues.class);
        when(docValues.getEncodedValue()).thenReturn(encodedDocVector);

        ScoreScript scoreScript = mock(ScoreScript.class);
        when(scoreScript.getDoc()).thenReturn(Collections.singletonMap(field, docValues));

        ScoreScriptUtils.HammingDistance function = new ScoreScriptUtils.HammingDistance(scoreScript, queryVector, field);
        int distance = function.hammingDistance();
        assertEquals(1, distance);
    }

    private static BytesRef mockEncodeBitVector(boolean[] docVector) {
        int length = (docVector.length - 1) / 8 + 1;
        byte[] bytes = new byte[length];
        int byteIndex = 0;

        for (int index = 0; index < docVector.length; index++) {
            int value = docVector[index] ? 1 : 0;
            int offset = index % 8;
            bytes[byteIndex] |= value << offset;

            if (offset == 7) {
                byteIndex++;
            }
        }

        return new BytesRef(bytes);
    }
}
