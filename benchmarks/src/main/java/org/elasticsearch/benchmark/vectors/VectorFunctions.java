/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */


package org.elasticsearch.benchmark.vectors;

import org.apache.lucene.util.BytesRef;

import java.nio.ByteBuffer;

final class VectorFunctions {
    static final byte INT_BYTES = 4;
    static final byte SHORT_BYTES = 2;

    private VectorFunctions() {}

    static float[] decodeNoop(BytesRef vectorBR) {
        if (vectorBR == null) {
            throw new IllegalArgumentException("A document doesn't have a value for a vector field!");
        }
        int dimCount = vectorBR.length / INT_BYTES;
        float[] vector = new float[dimCount];
        for (int dim = 0; dim < dimCount; dim++) {
            vector[dim] = 0.0f;
        }
        return vector;
    }

    static float[] decode(BytesRef vectorBR) {
        if (vectorBR == null) {
            throw new IllegalArgumentException("A document doesn't have a value for a vector field!");
        }
        int dimCount = vectorBR.length / INT_BYTES;
        float[] vector = new float[dimCount];
        int offset = vectorBR.offset;
        for (int dim = 0; dim < dimCount; dim++) {
            int intValue = ((vectorBR.bytes[offset++] & 0xFF) << 24)   |
                ((vectorBR.bytes[offset++] & 0xFF) << 16) |
                ((vectorBR.bytes[offset++] & 0xFF) <<  8) |
                (vectorBR.bytes[offset++] & 0xFF);
            vector[dim] = Float.intBitsToFloat(intValue);
        }
        return vector;
    }

    static float[] decodeWithByteBuffer(BytesRef vectorBR) {
        if (vectorBR == null) {
            throw new IllegalArgumentException("A document doesn't have a value for a vector field!");
        }
        int dimCount = vectorBR.length / INT_BYTES;
        float[] vector = new float[dimCount];
        ByteBuffer byteBuffer = ByteBuffer.wrap(vectorBR.bytes, vectorBR.offset, vectorBR.length);
        for (int dim = 0; dim < dimCount; dim++) {
            vector[dim] = byteBuffer.getFloat();
        }
        return vector;
    }

    static float[] decodeBFloat16(BytesRef vectorBR) {
        if (vectorBR == null) {
            throw new IllegalArgumentException("A document doesn't have a value for a vector field!");
        }
        int dimCount = vectorBR.length / SHORT_BYTES;
        float[] vector = new float[dimCount];
        int offset = vectorBR.offset;
        for (int dim = 0; dim < dimCount; dim++) {
            int intValue = ((vectorBR.bytes[offset++] & 0xFF) << 24) | ((vectorBR.bytes[offset++] & 0xFF) << 16);
            vector[dim] = Float.intBitsToFloat(intValue);
        }
        return vector;
    }

    static float[] decodeWithUnrolling4(BytesRef vectorBR) {
        if (vectorBR == null) {
            throw new IllegalArgumentException("A document doesn't have a value for a vector field!");
        }
        int dimCount = vectorBR.length / INT_BYTES;
        float[] vector = new float[dimCount];
        int offset = vectorBR.offset;
        for (int dim = 0; dim < dimCount; dim++) {
            int value1 = (vectorBR.bytes[offset] & 0xFF) << 24;
            int value2 = (vectorBR.bytes[offset + 1] & 0xFF) << 16;
            int value3 = (vectorBR.bytes[offset + 2] & 0xFF) << 8;
            int value4 = (vectorBR.bytes[offset + 3] & 0xFF);

            vector[dim] = Float.intBitsToFloat(value1 | value2 | value3 | value4);
            offset += 4;
        }
        return vector;
    }

    static float dotProduct(float[] v1, float[] v2){
        float dotProduct = 0;
        for (int dim = 0; dim < v2.length; dim++) {
            dotProduct += v1[dim] * v2[dim];
        }
        return dotProduct;
    }

    static float dotProductWithUnrolling4(float[] v1, float[] v2) {
        float dot0 = 0;
        float dot1 = 0;
        float dot2 = 0;
        float dot3 = 0;

        int length = (v1.length / 4) * 4;
        for (int dim = 0; dim < length; dim += 4) {
            dot0 += v1[dim] * v2[dim];
            dot1 += v1[dim + 1] * v2[dim + 1];
            dot2 += v1[dim + 2] * v2[dim + 2];
            dot3 += v1[dim + 3] * v2[dim + 3];
        }

        for (int dim = length; dim < v1.length; dim++) {
            dot0 += v1[dim] * v2[dim];
        }

        return dot0 + dot1 + dot2 + dot3;
    }

    static float decodeThenDotProduct(float[] queryVector, BytesRef vectorBR) {
        if (vectorBR == null) {
            throw new IllegalArgumentException("A document doesn't have a value for a vector field!");
        }
        int dimCount = vectorBR.length / INT_BYTES;
        float[] vector = new float[dimCount];

        ByteBuffer byteBuffer = ByteBuffer.wrap(vectorBR.bytes, vectorBR.offset, vectorBR.length);
        for (int dim = 0; dim < dimCount; dim++) {
            vector[dim] = byteBuffer.getFloat();
        }

        float dotProduct = 0.0f;
        for (int dim = 0; dim < queryVector.length; dim++) {
            dotProduct += queryVector[dim] * vector[dim];
        }
        return dotProduct;
    }

    static float decodeBFloat16ThenDotProduct(float[] queryVector, BytesRef vectorBR) {
        if (vectorBR == null) {
            throw new IllegalArgumentException("A document doesn't have a value for a vector field!");
        }
        int dimCount = vectorBR.length / SHORT_BYTES;
        float[] vector = new float[dimCount];
        int offset = vectorBR.offset;
        for (int dim = 0; dim < dimCount; dim++) {
            int intValue = ((vectorBR.bytes[offset++] & 0xFF) << 24) | ((vectorBR.bytes[offset++] & 0xFF) << 16);
            vector[dim] = Float.intBitsToFloat(intValue);
        }

        float dotProduct = 0.0f;
        for (int dim = 0; dim < queryVector.length; dim++) {
            dotProduct += queryVector[dim] * vector[dim];
        }
        return dotProduct;
    }

    static float decodeAndDotProduct(float[] queryVector, BytesRef vectorBR) {
        if (vectorBR == null) {
            throw new IllegalArgumentException("A document doesn't have a value for a vector field!");
        }

        ByteBuffer byteBuffer = ByteBuffer.wrap(vectorBR.bytes, vectorBR.offset, vectorBR.length);
        float dotProduct = 0.0f;

        for (float value : queryVector) {
            dotProduct += value * byteBuffer.getFloat();
        }
        return dotProduct;
    }

    static float decodeBFloat16AndDotProduct(float[] queryVector, BytesRef vectorBR) {
        if (vectorBR == null) {
            throw new IllegalArgumentException("A document doesn't have a value for a vector field!");
        }
        int offset = vectorBR.offset;
        float dotProduct = 0.0f;
        for (float value : queryVector) {
            int intValue = ((vectorBR.bytes[offset++] & 0xFF) << 24)  | ((vectorBR.bytes[offset++] & 0xFF) << 16);
            dotProduct += value * Float.intBitsToFloat(intValue);
        }
        return dotProduct;
    }

    static float decodeAndDotProductWithUnrolling2(float[] queryVector, BytesRef vectorBR) {
        if (vectorBR == null) {
            throw new IllegalArgumentException("A document doesn't have a value for a vector field!");
        }

        float dot0 = 0;
        float dot1 = 0;

        ByteBuffer byteBuffer = ByteBuffer.wrap(vectorBR.bytes, vectorBR.offset, vectorBR.length);
        int offset = vectorBR.offset;
        int length = (queryVector.length / 2) * 2;

        for (int dim = 0; dim < length; dim += 2, offset += 8) {
            dot0 += byteBuffer.getFloat(offset) * queryVector[dim];
            dot1 += byteBuffer.getFloat(offset + 4) * queryVector[dim + 1];
        }

        for (int dim = length; dim < queryVector.length; dim++, offset += 4) {
            dot0 += byteBuffer.getFloat(offset) * queryVector[dim];
        }
        return dot0 + dot1;
    }

    static float decodeAndDotProductWithUnrolling4(float[] queryVector, BytesRef vectorBR) {
        if (vectorBR == null) {
            throw new IllegalArgumentException("A document doesn't have a value for a vector field!");
        }

        float dot0 = 0;
        float dot1 = 0;
        float dot2 = 0;
        float dot3 = 0;

        ByteBuffer byteBuffer = ByteBuffer.wrap(vectorBR.bytes, vectorBR.offset, vectorBR.length);
        int offset = vectorBR.offset;
        int length = (queryVector.length / 4) * 4;

        for (int dim = 0; dim < length; dim += 4, offset += 16) {
            dot0 += byteBuffer.getFloat(offset) * queryVector[dim];
            dot1 += byteBuffer.getFloat(offset + 4) * queryVector[dim + 1];
            dot2 += byteBuffer.getFloat(offset + 8) * queryVector[dim + 2];
            dot3 += byteBuffer.getFloat(offset + 12) * queryVector[dim + 3];
        }

        for (int dim = length; dim < queryVector.length; dim++, offset += 4) {
            dot0 += byteBuffer.getFloat(offset) * queryVector[dim];
        }
        return dot0 + dot1 + dot2 + dot3;
    }
}
