/*
 *   Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License").
 *   You may not use this file except in compliance with the License.
 *   A copy of the License is located at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   or in the "license" file accompanying this file. This file is distributed
 *   on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *   express or implied. See the License for the specific language governing
 *   permissions and limitations under the License.
 */

package org.elasticsearch.xpack.vectors.codec;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.CompoundFormat;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.FieldInfosFormat;
import org.apache.lucene.codecs.LiveDocsFormat;
import org.apache.lucene.codecs.NormsFormat;
import org.apache.lucene.codecs.PointsFormat;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.SegmentInfoFormat;
import org.apache.lucene.codecs.StoredFieldsFormat;
import org.apache.lucene.codecs.TermVectorsFormat;

public final class KMeansCodec extends Codec {

    private static final String CODEC_NAME = "KMeansCodec";

    public KMeansCodec() {
        super(CODEC_NAME);
    }

    private Codec delegate() {
        return Codec.getDefault();
    }

    @Override
    public PostingsFormat postingsFormat() {
        return delegate().postingsFormat();
    }

    @Override
    public DocValuesFormat docValuesFormat() {
        DocValuesFormat docValuesFormat = delegate().docValuesFormat();
        return new KMeansDocValuesFormat(CODEC_NAME, docValuesFormat);
    }

    @Override
    public StoredFieldsFormat storedFieldsFormat() {
        return delegate().storedFieldsFormat();
    }

    @Override
    public TermVectorsFormat termVectorsFormat() {
        return delegate().termVectorsFormat();
    }

    @Override
    public FieldInfosFormat fieldInfosFormat() {
        return delegate().fieldInfosFormat();
    }

    @Override
    public SegmentInfoFormat segmentInfoFormat() {
        return delegate().segmentInfoFormat();
    }

    @Override
    public NormsFormat normsFormat() {
        return delegate().normsFormat();
    }

    @Override
    public LiveDocsFormat liveDocsFormat() {
        return delegate().liveDocsFormat();
    }

    @Override
    public CompoundFormat compoundFormat() {
        return delegate().compoundFormat();
    }

    @Override
    public PointsFormat pointsFormat() {
        return delegate().pointsFormat();
    }
}
