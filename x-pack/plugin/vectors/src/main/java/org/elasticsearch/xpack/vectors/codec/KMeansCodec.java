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
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.FilterCodec;

public final class KMeansCodec extends FilterCodec {

    private static final String CODEC_NAME = "KMeansCodec";
    static final String EXTENSION_NAME = ".kmeans";

    public KMeansCodec() {
        super(CODEC_NAME, Codec.getDefault());
    }

    @Override
    public DocValuesFormat docValuesFormat() {
        DocValuesFormat delegate = super.docValuesFormat();
        return new KMeansDocValuesFormat(CODEC_NAME, delegate);
    }
}
