/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.elasticsearch.xpack.vectors.mapper;

import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.bkd.BKDReader;

import java.io.IOException;
import java.util.Arrays;
import java.util.PriorityQueue;

/**
 * A copy of {@link FloatPointNearestNeighbor} from lucene sandbox,
 * modified to return {@link NearestHit} objects directly.
 */
public class FloatPointNearestNeighbor {

    static class Cell implements Comparable<Cell> {
        final int readerIndex;
        final byte[] minPacked;
        final byte[] maxPacked;
        final BKDReader.IndexTree index;

        /**
         * The closest possible distance^2 of all points in this cell
         */
        final double distanceSquared;

        Cell(BKDReader.IndexTree index, int readerIndex, byte[] minPacked, byte[] maxPacked, double distanceSquared) {
            this.index = index;
            this.readerIndex = readerIndex;
            this.minPacked = minPacked.clone();
            this.maxPacked = maxPacked.clone();
            this.distanceSquared = distanceSquared;
        }

        public int compareTo(Cell other) {
            return Double.compare(distanceSquared, other.distanceSquared);
        }

        @Override
        public String toString() {
            return "Cell(readerIndex=" + readerIndex + " nodeID=" + index.getNodeID()
                + " isLeaf=" + index.isLeafNode() + " distanceSquared=" + distanceSquared + ")";
        }
    }

    private static class NearestVisitor implements PointValues.IntersectVisitor {
        int curDocBase;
        Bits curLiveDocs;
        final int topN;
        final PriorityQueue<NearestHit> hitQueue;
        final float[] origin;
        private int dims;
        private int updateMinMaxCounter;
        private float[] min;
        private float[] max;


        public NearestVisitor(PriorityQueue<NearestHit> hitQueue, int topN, float[] origin) {
            this.hitQueue = hitQueue;
            this.topN = topN;
            this.origin = origin;
            dims = origin.length;
            min = new float[dims];
            max = new float[dims];
            Arrays.fill(min, Float.NEGATIVE_INFINITY);
            Arrays.fill(max, Float.POSITIVE_INFINITY);
        }

        @Override
        public void visit(int docID) {
            throw new AssertionError();
        }

        private static final int MANTISSA_BITS = 23;

        /**
         * Returns the minimum value that will change the given distance when added to it.
         * <p>
         * This value is calculated from the distance exponent reduced by (at most) 23,
         * the number of bits in a float mantissa. This is necessary when the result of
         * subtracting/adding the distance in a single dimension has an exponent that
         * differs significantly from that of the distance value. Without this fudge
         * factor (i.e. only subtracting/adding the distance), cells and values can be
         * inappropriately judged as outside the search radius.
         */
        private float getMinDelta(float distance) {
            int exponent = Float.floatToIntBits(distance) >> MANTISSA_BITS; // extract biased exponent (distance is positive)
            if (exponent == 0) {
                return Float.MIN_VALUE;
            } else {
                exponent = exponent <= MANTISSA_BITS ? 1 : exponent - MANTISSA_BITS; // Avoid underflow
                return Float.intBitsToFloat(exponent << MANTISSA_BITS);
            }
        }

        private void maybeUpdateMinMax() {
            if (updateMinMaxCounter < 1024 || (updateMinMaxCounter & 0x3F) == 0x3F) {
                NearestHit hit = hitQueue.peek();
                float distance = (float) Math.sqrt(hit.distanceSquared);
                float minDelta = getMinDelta(distance);
                // String oldMin = Arrays.toString(min);
                // String oldMax = Arrays.toString(max);
                for (int d = 0; d < dims; ++d) {
                    min[d] = (origin[d] - distance) - minDelta;
                    max[d] = (origin[d] + distance) + minDelta;
                    // System.out.println("origin[" + d + "] (" + origin[d] + ") - distance (" + distance + ") - minDelta (" + minDelta +
                    // ") = min[" + d + "] (" + min[d] + ")");
                    // System.out.println("origin[" + d + "] (" + origin[d] + ") + distance (" + distance + ") + minDelta (" + minDelta +
                    // ") = max[" + d + "] (" + max[d] + ")");
                }
                // System.out.println("maybeUpdateMinMax:  min: " + oldMin + " -> " + Arrays.toString(min) + "   max: " + oldMax + " -> "
                // + Arrays.toString(max));
            }
            ++updateMinMaxCounter;
        }

        @Override
        public void visit(int docID, byte[] packedValue) {
            // System.out.println("visit docID=" + docID + " liveDocs=" + curLiveDocs);

            if (curLiveDocs != null && curLiveDocs.get(docID) == false) {
                return;
            }

            float[] docPoint = new float[dims];
            for (int d = 0, offset = 0; d < dims; ++d, offset += Float.BYTES) {
                docPoint[d] = FloatPoint.decodeDimension(packedValue, offset);
                if (docPoint[d] > max[d] || docPoint[d] < min[d]) {

                    // if (docPoint[d] > max[d]) {
                    //   System.out.println("  skipped because docPoint[" + d + "] (" + docPoint[d] + ") > max[" + d + "] (" + max[d] +
                    //   ")");
                    // } else {
                    //   System.out.println("  skipped because docPoint[" + d + "] (" + docPoint[d] + ") < min[" + d + "] (" + min[d] +
                    //   ")");
                    // }

                    return;
                }
            }

            double distanceSquared = euclideanDistanceSquared(origin, docPoint);

            // System.out.println("    visit docID=" + docID + " distanceSquared=" + distanceSquared + " value: " + Arrays.toString
            // (docPoint));

            int fullDocID = curDocBase + docID;

            if (hitQueue.size() == topN) { // queue already full
                NearestHit bottom = hitQueue.peek();
                // System.out.println("      bottom distanceSquared=" + bottom.distanceSquared);
                if (distanceSquared < bottom.distanceSquared
                    // we don't collect docs in order here, so we must also test the tie-break case ourselves:
                    || (distanceSquared == bottom.distanceSquared && fullDocID < bottom.docID)) {
                    hitQueue.poll();
                    bottom.docID = fullDocID;
                    bottom.distanceSquared = distanceSquared;
                    hitQueue.offer(bottom);
                    // System.out.println("      ** keep1, now bottom=" + bottom);
                    maybeUpdateMinMax();
                }
            } else {
                NearestHit hit = new NearestHit();
                hit.docID = fullDocID;
                hit.distanceSquared = distanceSquared;
                hitQueue.offer(hit);
                // System.out.println("      ** keep2, new addition=" + hit);
            }
        }

        @Override
        public PointValues.Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
            return PointValues.Relation.CELL_CROSSES_QUERY;
        }
    }

    /**
     * Holds one hit from {@link FloatPointNearestNeighbor#nearest}
     */
    static class NearestHit {
        public int docID;
        public double distanceSquared;

        @Override
        public String toString() {
            return "NearestHit(docID=" + docID + " distanceSquared=" + distanceSquared + ")";
        }
    }

    private static NearestHit[] nearest(BKDReader reader, Bits liveDocs, Integer docBasis, final int topN, float[] origin) throws IOException {

        // System.out.println("NEAREST: readers=" + readers + " liveDocs=" + liveDocs + " origin: " + Arrays.toString(origin));

        // Holds closest collected points seen so far:
        // TODO: if we used lucene's PQ we could just updateTop instead of poll/offer:
        final PriorityQueue<NearestHit> hitQueue = new PriorityQueue<>(topN, (a, b) -> {
            // sort by opposite distance natural order
            int cmp = Double.compare(a.distanceSquared, b.distanceSquared);
            return cmp != 0 ? -cmp : b.docID - a.docID; // tie-break by higher docID
        });

        // Holds all cells, sorted by closest to the point:
        PriorityQueue<Cell> cellQueue = new PriorityQueue<>();

        NearestVisitor visitor = new NearestVisitor(hitQueue, topN, origin);
        visitor.curDocBase = docBasis;
        visitor.curLiveDocs = liveDocs;

        int bytesPerDim = reader.getBytesPerDimension();

        byte[] minPackedValue = reader.getMinPackedValue();
        byte[] maxPackedValue = reader.getMaxPackedValue();
        BKDReader.IntersectState state = reader.getIntersectState(visitor);

        cellQueue.offer(new Cell(state.index, 0, reader.getMinPackedValue(), reader.getMaxPackedValue(),
            approxBestDistanceSquared(minPackedValue, maxPackedValue, origin)));

        LOOP_OVER_CELLS:
        while (cellQueue.size() > 0) {
            Cell cell = cellQueue.poll();
            // System.out.println("  visit " + cell);

            // TODO: if we replace approxBestDistance with actualBestDistance, we can put an opto here to break once this "best" cell is
            //  fully outside of the hitQueue bottom's radius:

            if (cell.index.isLeafNode()) {
                // System.out.println("    leaf");
                // Leaf block: visit all points and possibly collect them:
                reader.visitLeafBlockValues(cell.index, state);
                // System.out.println("    now " + hitQueue.size() + " hits");
            } else {
                // System.out.println("    non-leaf");
                // Non-leaf block: split into two cells and put them back into the queue:

                if (hitQueue.size() == topN) {
                    for (int d = 0, offset = 0; d < visitor.dims; ++d, offset += Float.BYTES) {
                        float cellMaxAtDim = FloatPoint.decodeDimension(cell.maxPacked, offset);
                        float cellMinAtDim = FloatPoint.decodeDimension(cell.minPacked, offset);
                        if (cellMaxAtDim < visitor.min[d] || cellMinAtDim > visitor.max[d]) {
                            // this cell is outside our search radius; don't bother exploring any more

                            // if (cellMaxAtDim < visitor.min[d]) {
                            //   System.out.println("  skipped because cell max at " + d + " (" + cellMaxAtDim + ") < visitor.min[" + d +
                            //   "] (" + visitor.min[d] + ")");
                            // } else {
                            //   System.out.println("  skipped because cell min at " + d + " (" + cellMinAtDim + ") > visitor.max[" + d +
                            //   "] (" + visitor.max[d] + ")");
                            // }

                            continue LOOP_OVER_CELLS;
                        }
                    }
                }
                BytesRef splitValue = BytesRef.deepCopyOf(cell.index.getSplitDimValue());
                int splitDim = cell.index.getSplitDim();

                // we must clone the index so that we we can recurse left and right "concurrently":
                BKDReader.IndexTree newIndex = cell.index.clone();
                byte[] splitPackedValue = cell.maxPacked.clone();
                System.arraycopy(splitValue.bytes, splitValue.offset, splitPackedValue, splitDim * bytesPerDim, bytesPerDim);

                cell.index.pushLeft();
                cellQueue.offer(new Cell(cell.index, cell.readerIndex, cell.minPacked, splitPackedValue,
                    approxBestDistanceSquared(cell.minPacked, splitPackedValue, origin)));

                splitPackedValue = cell.minPacked.clone();
                System.arraycopy(splitValue.bytes, splitValue.offset, splitPackedValue, splitDim * bytesPerDim, bytesPerDim);

                newIndex.pushRight();
                cellQueue.offer(new Cell(newIndex, cell.readerIndex, splitPackedValue, cell.maxPacked,
                    approxBestDistanceSquared(splitPackedValue, cell.maxPacked, origin)));
            }
        }

        NearestHit[] hits = new NearestHit[hitQueue.size()];
        int downTo = hitQueue.size() - 1;
        while (hitQueue.size() != 0) {
            hits[downTo] = hitQueue.poll();
            downTo--;
        }
        return hits;
    }

    private static double approxBestDistanceSquared(byte[] minPackedValue, byte[] maxPackedValue, float[] value) {
        boolean insideCell = true;
        float[] min = new float[value.length];
        float[] max = new float[value.length];
        double[] closest = new double[value.length];
        for (int i = 0, offset = 0; i < value.length; ++i, offset += Float.BYTES) {
            min[i] = FloatPoint.decodeDimension(minPackedValue, offset);
            max[i] = FloatPoint.decodeDimension(maxPackedValue, offset);
            if (insideCell) {
                if (value[i] < min[i] || value[i] > max[i]) {
                    insideCell = false;
                }
            }
            double minDiff = Math.abs((double) value[i] - (double) min[i]);
            double maxDiff = Math.abs((double) value[i] - (double) max[i]);
            closest[i] = minDiff < maxDiff ? minDiff : maxDiff;
        }
        if (insideCell) {
            return 0.0f;
        }
        double sumOfSquaredDiffs = 0.0d;
        for (int d = 0; d < value.length; ++d) {
            sumOfSquaredDiffs += closest[d] * closest[d];
        }
        return sumOfSquaredDiffs;
    }

    public static double euclideanDistanceSquared(float[] a, float[] b) {
        double sumOfSquaredDifferences = 0.0d;
        for (int d = 0; d < a.length; ++d) {
            double diff = (double) a[d] - (double) b[d];
            sumOfSquaredDifferences += diff * diff;
        }
        return sumOfSquaredDifferences;
    }

    public static NearestHit[] nearest(LeafReaderContext context,
                                       String field,
                                       int topN,
                                       float... origin) throws IOException {

        PointValues points = context.reader().getPointValues(field);
        if (points == null) {
            return new NearestHit[0];
        }

        if (points instanceof BKDReader == false) {
            throw new IllegalArgumentException("can only run on Lucene60PointsReader points implementation, but got " + points);
        }

        BKDReader reader = (BKDReader) points;
        Bits liveDocs = context.reader().getLiveDocs();
        Integer docBase = context.docBase;

        return nearest(reader, liveDocs, docBase, topN, origin);
    }
}
