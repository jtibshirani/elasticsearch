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

        @Setup
        public void createVectors() {
            this.queryVector = randomVector();
            this.vector = randomVector();
            this.encodedVector = encode(vector);
        }
    }

    @Benchmark
    public float[] decodeNoop(VectorState state) {
        return VectorFunctions.decodeNoop(state.encodedVector);
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
    public float[] decodeWithUnrolling4(VectorState state) {
        return VectorFunctions.decodeWithUnrolling4(state.encodedVector);
    }

    @Benchmark
    public float dotProduct(VectorState state) {
        return VectorFunctions.dotProduct(state.queryVector, state.vector);
    }

    @Benchmark
    public float dotProductWithUnrolling4(VectorState state) {
        return VectorFunctions.dotProductWithUnrolling4(state.queryVector, state.vector);
    }

    @Benchmark
    public float decodeThenDotProduct(VectorState state) {
        return VectorFunctions.decodeThenDotProduct(state.queryVector, state.encodedVector);
    }

    @Benchmark
    public float decodeAndDotProduct(VectorState state) {
        return VectorFunctions.decodeAndDotProduct(state.queryVector, state.encodedVector);
    }

    @Benchmark
    public float decodeAndDotProductWithUnrolling2(VectorState state) {
        return VectorFunctions.decodeAndDotProductWithUnrolling2(state.queryVector, state.encodedVector);
    }

    @Benchmark
    public float decodeAndDotProductWithUnrolling4(VectorState state) {
        return VectorFunctions.decodeAndDotProductWithUnrolling4(state.queryVector, state.encodedVector);
    }

    @Benchmark
    @Warmup(iterations = 0)
    @Measurement(iterations = 1)
    public void testVectorFunctions(VectorState state) {
        float[] queryVector = state.queryVector;
        float[] vector = state.vector;
        BytesRef encodedVector = state.encodedVector;

        assertEquals(vector, VectorFunctions.decode(encodedVector));
        assertEquals(vector, VectorFunctions.decodeWithByteBuffer(encodedVector));
        assertEquals(vector, VectorFunctions.decodeWithUnrolling4(encodedVector));

        float dotProduct = VectorFunctions.dotProduct(queryVector, vector);
        assertEquals(dotProduct, VectorFunctions.dotProductWithUnrolling4(queryVector, vector));
        assertEquals(dotProduct, VectorFunctions.decodeThenDotProduct(queryVector, encodedVector));
        assertEquals(dotProduct, VectorFunctions.decodeAndDotProduct(queryVector, encodedVector));
        assertEquals(dotProduct, VectorFunctions.decodeAndDotProductWithUnrolling2(queryVector, encodedVector));
        assertEquals(dotProduct, VectorFunctions.decodeAndDotProductWithUnrolling4(queryVector, encodedVector));
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
            buf[offset++] =  (byte) (intValue >> 24);
            buf[offset++] = (byte) (intValue >> 16);
            buf[offset++] = (byte) (intValue >>  8);
            buf[offset++] = (byte) intValue;
        }
        return new BytesRef(buf, 0, offset);
    }

}
