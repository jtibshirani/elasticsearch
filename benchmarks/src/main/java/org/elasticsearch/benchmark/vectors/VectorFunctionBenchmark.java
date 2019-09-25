/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.benchmark.vectors;

import org.apache.lucene.util.BytesRef;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.util.Random;
import java.util.concurrent.TimeUnit;

@Fork(3)
@Warmup(iterations = 10)
@Measurement(iterations = 10)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
@SuppressWarnings("unused") //invoked by benchmarking framework
public class VectorFunctionBenchmark {
    private final static int DIMS = 101;

    @State(Scope.Thread)
    public static class VectorState {
        private float[] queryVector;
        private float[] vector;
        private BytesRef encodedVector;
        private BytesRef encodedVectorBF16;

        @Setup
        public void createVectors() {
            this.queryVector = randomVector();
            this.vector = randomVector();
            this.encodedVector = encode(vector);
            this.encodedVectorBF16 = encodeBFloat16(vector);
        }
    }

    @Benchmark
    public float[] decode(VectorState state) {
        return VectorFunctions.decode(state.encodedVector);
    }

    @Benchmark
    public float[] decodeWithByteBuffer(VectorState state) {
        return VectorFunctions.decodeWithByteBuffer(state.encodedVector);
    }


    @Benchmark
    public float[] decodeBFloat16(VectorState state) {
        return VectorFunctions.decodeBFloat16(state.encodedVectorBF16);
    }

    @Benchmark
    public float dotProduct(VectorState state) {
        return VectorFunctions.dotProduct(state.queryVector, state.vector);
    }

    @Benchmark
    public float decodeThenDotProduct(VectorState state) {
        return VectorFunctions.decodeThenDotProduct(state.queryVector, state.encodedVector);
    }

    @Benchmark
    public float decodeBFloat16ThenDotProduct(VectorState state) {
        return VectorFunctions.decodeBFloat16ThenDotProduct(state.queryVector, state.encodedVectorBF16);
    }

    @Benchmark
    public float decodeAndDotProduct(VectorState state) {
        return VectorFunctions.decodeAndDotProduct(state.queryVector, state.encodedVector);
    }

    @Benchmark
    public float decodeBFloat16AndDotProduct(VectorState state) {
        return VectorFunctions.decodeBFloat16AndDotProduct(state.queryVector, state.encodedVectorBF16);
    }

    @Benchmark
    @Warmup(iterations = 0)
    @Measurement(iterations = 1)
    public void testVectorFunctions(VectorState state) {
        float[] queryVector = state.queryVector;
        float[] vector = state.vector;
        BytesRef encodedVector = state.encodedVector;
        BytesRef encodedVectorBF16 = state.encodedVectorBF16;

        assertEquals(vector, VectorFunctions.decode(encodedVector));
        assertEquals(vector, VectorFunctions.decodeWithByteBuffer(encodedVector));
        assertAlmostEquals(vector, VectorFunctions.decodeBFloat16(encodedVectorBF16));

        float dotProduct = VectorFunctions.dotProduct(queryVector, vector);
        assertEquals(dotProduct, VectorFunctions.decodeThenDotProduct(queryVector, encodedVector));
        assertEquals(dotProduct, VectorFunctions.decodeAndDotProduct(queryVector, encodedVector));
        assertAlmostEquals(dotProduct, VectorFunctions.decodeBFloat16ThenDotProduct(queryVector, encodedVectorBF16));
        assertAlmostEquals(dotProduct, VectorFunctions.decodeBFloat16AndDotProduct(queryVector, encodedVectorBF16));
    }

    private void assertEquals(float a, float b) {
        if (Math.abs(a - b) > 0.0001f) {
            throw new RuntimeException();
        }
    }

    private void assertEquals(float[] a, float[] b) {
        if (a.length != b.length) {
            throw new RuntimeException();
        }
        for (int i = 0; i < a.length; i++) {
            assertEquals(a[i], b[i]);
        }
    }

    private void assertAlmostEquals(float a, float b) {
        float errorThreshold = a / 100; // keep relative error of about 1%.
        if (Math.abs(a - b) > errorThreshold) {
            throw new RuntimeException();
        }
    }

    private void assertAlmostEquals(float[] a, float[] b) {
        if (a.length != b.length) {
            throw new RuntimeException();
        }
        for (int i = 0; i < a.length; i++) {
            assertAlmostEquals(a[i], b[i]);
        }
    }

    private static float[] randomVector() {
        Random random = new Random();
        float[] result = new float[DIMS];
        for (int i = 0; i < result.length; i++) {
            result[i] = random.nextFloat();
        }
        return result;
    }

    public static BytesRef encode(float[] values) {
        final short INT_BYTES = VectorFunctions.INT_BYTES;
        byte[] buf = new byte[INT_BYTES * values.length];
        int offset = 0;
        int intValue;
        for (float value: values) {
            intValue = Float.floatToIntBits(value);
            buf[offset++] = (byte) (intValue >> 24);
            buf[offset++] = (byte) (intValue >> 16);
            buf[offset++] = (byte) (intValue >>  8);
            buf[offset++] = (byte) intValue;
        }
        return new BytesRef(buf, 0, offset);
    }

    // encode using bfloat16
    public static BytesRef encodeBFloat16(float[] values) {
        final short SHORT_BYTES = VectorFunctions.SHORT_BYTES;
        byte[] buf = new byte[SHORT_BYTES * values.length];
        int offset = 0;
        int intValue;
        for (float value: values) {
            intValue = Float.floatToIntBits(value);
            buf[offset++] = (byte) (intValue >> 24);
            buf[offset++] = (byte) (intValue >> 16);
        }
        return new BytesRef(buf, 0, offset);
    }

}
