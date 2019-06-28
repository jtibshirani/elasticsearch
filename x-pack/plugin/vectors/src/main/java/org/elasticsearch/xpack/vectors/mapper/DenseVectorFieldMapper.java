/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */


package org.elasticsearch.xpack.vectors.mapper;

import com.carrotsearch.hppc.IntFloatHashMap;
import com.carrotsearch.hppc.IntFloatMap;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.DocValuesFieldExistsQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.DocIdSetBuilder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser.Token;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.mapper.ArrayValueMapperParser;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.xpack.vectors.query.VectorDVIndexFieldData;

import java.io.IOException;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

/**
 * A {@link FieldMapper} for indexing a dense vector of floats.
 */
public class DenseVectorFieldMapper extends FieldMapper implements ArrayValueMapperParser {
    private static final int DEFAULT_PROJECTIONS = 4;
    private static final int DEFAULT_TOP_HITS = 10;

    public static final String CONTENT_TYPE = "dense_vector";
    public static short MAX_DIMS_COUNT = 1024; //maximum allowed number of dimensions
    private static final byte INT_BYTES = 4;

    public static class Defaults {
        public static final MappedFieldType FIELD_TYPE = new DenseVectorFieldType();

        static {
            FIELD_TYPE.setTokenized(false);
            FIELD_TYPE.setIndexOptions(IndexOptions.NONE);
            FIELD_TYPE.setHasDocValues(true);
            FIELD_TYPE.setOmitNorms(true);
            FIELD_TYPE.freeze();
        }
    }

    public static class Builder extends FieldMapper.Builder<Builder, DenseVectorFieldMapper> {
        private int dims = 0;
        private int projections = 0;
        private int topHits = 0;

        public Builder(String name) {
            super(name, Defaults.FIELD_TYPE, Defaults.FIELD_TYPE);
            builder = this;
        }

        public Builder dims(int dims) {
            if ((dims > MAX_DIMS_COUNT) || (dims < 1)) {
                throw new MapperParsingException("The number of dimensions for field [" + name +
                    "] should be in the range [1, " + MAX_DIMS_COUNT + "]");
            }
            this.dims = dims;
            return this;
        }

        /**
         * The number of projections (random trees) to create.
         */
        public Builder projections(int projections) {
            this.projections = projections;
            return this;
        }

        /**
         * The number of candidate hits to retrieve from each tree.
         */
        public Builder topHits(int topHits) {
            this.topHits = topHits;
            return this;
        }

        @Override
        protected void setupFieldType(BuilderContext context) {
            super.setupFieldType(context);
            fieldType().setDims(dims);
            fieldType().randomProjector = RandomProjector.create(dims, projections);
            fieldType().topHits = topHits;
            fieldType().projections = projections;
        }

        @Override
        public DenseVectorFieldType fieldType() {
            return (DenseVectorFieldType) super.fieldType();
        }

        @Override
        public DenseVectorFieldMapper build(BuilderContext context) {
            setupFieldType(context);

            return new DenseVectorFieldMapper(
                    name, fieldType, defaultFieldType,
                    context.indexSettings(), multiFieldsBuilder.build(this, context), copyTo);
        }
    }

    public static class TypeParser implements Mapper.TypeParser {
        @Override
        public Mapper.Builder<?,?> parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            DenseVectorFieldMapper.Builder builder = new DenseVectorFieldMapper.Builder(name);
            Object dimsField = node.remove("dims");
            if (dimsField == null) {
                throw new MapperParsingException("The [dims] property must be specified for field [" + name + "].");
            }
            int dims = XContentMapValues.nodeIntegerValue(dimsField);
            builder = builder.dims(dims);

            Object projectionsField = node.remove("projections");
            int projections = XContentMapValues.nodeIntegerValue(projectionsField, DEFAULT_PROJECTIONS);
            builder = builder.projections(projections);

            Object topHitsField = node.remove("top_hits");
            int topHits = XContentMapValues.nodeIntegerValue(topHitsField, DEFAULT_TOP_HITS);
            builder = builder.topHits(topHits);

            return builder;
        }
    }

    public static final class DenseVectorFieldType extends MappedFieldType {
        private int dims;
        private RandomProjector randomProjector;
        public int topHits;
        public int projections;

        public DenseVectorFieldType() {}

        protected DenseVectorFieldType(DenseVectorFieldType ref) {
            super(ref);
            this.randomProjector = ref.randomProjector;
        }

        public DenseVectorFieldType clone() {
            return new DenseVectorFieldType(this);
        }

        int dims() {
            return dims;
        }

        void setDims(int dims) {
            this.dims = dims;
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public DocValueFormat docValueFormat(String format, ZoneId timeZone) {
            throw new UnsupportedOperationException(
                "Field [" + name() + "] of type [" + typeName() + "] doesn't support docvalue_fields or aggregations");
        }

        @Override
        public Query existsQuery(QueryShardContext context) {
            return new DocValuesFieldExistsQuery(name());
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(String fullyQualifiedIndexName) {
            return new VectorDVIndexFieldData.Builder(true);
        }

        // For prototype, represent an ANN query using a terms query (huge hack).
        @Override
        public Query termsQuery(List<?> values, QueryShardContext context) {
            float[] vector = new float[values.size()];
            for (int i = 0; i < values.size(); i++) {
                Object value = values.get(i);
                if (value instanceof Float) {
                    vector[i] = (float) value;
                } else {
                    vector[i] = (float) (double) value;
                }
            }

            List<float[]> projections = randomProjector.project(vector);
            return new AnnQuery(name(), projections, topHits);
        }

        @Override
        public Query termQuery(Object value, QueryShardContext context) {
            throw new UnsupportedOperationException();
        }
    }

    private static class AnnQuery extends Query {
        private final String field;
        private final List<float[]> projections;
        private final int topHits;

        public AnnQuery(String field,
                        List<float[]> projections,
                        int topHits) {
            this.field = field;
            this.projections = projections;
            this.topHits = topHits;
        }

        @Override
        public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
            return new ConstantScoreWeight(this, 1.0f) {

                @Override
                public boolean isCacheable(LeafReaderContext ctx) {
                    return false;
                }

                @Override
                public Scorer scorer(LeafReaderContext context) throws IOException {
                    DocIdSetBuilder builder = new DocIdSetBuilder(context.reader().maxDoc());

                    for (int i = 0; i < projections.size(); i++) {
                        String fieldName = field + "_" + i;
                        float[] projection = projections.get(i);
                        FloatPointNearestNeighbor.NearestHit[] hits = FloatPointNearestNeighbor.nearest(
                            context, fieldName, topHits, projection);


                        for (FloatPointNearestNeighbor.NearestHit hit : hits) {
                            int docId = hit.docID - context.docBase;
                            builder.grow(1).add(docId);
                        }
                    }

                    DocIdSetIterator iterator = builder.build().iterator();
                    return new ConstantScoreScorer(this, score(), scoreMode, iterator);
                }
            };
        }

        @Override
        public String toString(String field) {
            return "ann query on [" + field + "]";
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            AnnQuery annQuery = (AnnQuery) o;
            return Objects.equals(projections, annQuery.projections);
        }

        @Override
        public int hashCode() {
            return Objects.hash(projections);
        }
    }

    private DenseVectorFieldMapper(String name, MappedFieldType fieldType, MappedFieldType defaultFieldType,
                                   Settings indexSettings, MultiFields multiFields, CopyTo copyTo) {
        super(name, fieldType, defaultFieldType, indexSettings, multiFields, copyTo);
        assert fieldType.indexOptions() == IndexOptions.NONE;
    }

    @Override
    protected DenseVectorFieldMapper clone() {
        return (DenseVectorFieldMapper) super.clone();
    }

    @Override
    public DenseVectorFieldType fieldType() {
        return (DenseVectorFieldType) super.fieldType();
    }

    @Override
    public void parse(ParseContext context) throws IOException {
        if (context.externalValueSet()) {
            throw new IllegalArgumentException("Field [" + name() + "] of type [" + typeName() + "] can't be used in multi-fields");
        }
        int dims = fieldType().dims(); //number of vector dimensions

        // encode array of floats as array of integers and store into buf
        // this code is here and not int the VectorEncoderDecoder so not to create extra arrays
        byte[] buf = new byte[dims * INT_BYTES];
        int offset = 0;
        int dim = 0;
        for (Token token = context.parser().nextToken(); token != Token.END_ARRAY; token = context.parser().nextToken()) {
            if (dim++ >= dims) {
                throw new IllegalArgumentException("Field [" + name() + "] of type [" + typeName() + "] of doc [" +
                    context.sourceToParse().id() + "] has exceeded the number of dimensions [" + dims + "] defined in mapping");
            }
            ensureExpectedToken(Token.VALUE_NUMBER, token, context.parser()::getTokenLocation);
            float value = context.parser().floatValue(true);
            int intValue = Float.floatToIntBits(value);
            buf[offset++] = (byte) (intValue >> 24);
            buf[offset++] = (byte) (intValue >> 16);
            buf[offset++] = (byte) (intValue >>  8);
            buf[offset++] = (byte) intValue;
        }
        if (dim != dims) {
            throw new IllegalArgumentException("Field [" + name() + "] of type [" + typeName() + "] of doc [" +
                context.sourceToParse().id() + "] has number of dimensions [" + dim +
                "] less than defined in the mapping [" +  dims +"]");
        }

        BytesRef bytesRef = new BytesRef(buf, 0, offset);
        BinaryDocValuesField field = new BinaryDocValuesField(fieldType().name(), bytesRef);
        if (context.doc().getByKey(fieldType().name()) != null) {
            throw new IllegalArgumentException("Field [" + name() + "] of type [" + typeName() +
                "] doesn't not support indexing multiple values for the same field in the same document");
        }

        float[] vector = VectorEncoderDecoder.decodeDenseVector(bytesRef);
        List<float[]> projectedVectors = fieldType().randomProjector.project(vector);
        for (int i = 0; i < projectedVectors.size(); i++) {
            String fieldName = name() + "_" + i;
            float[] projectedVector = projectedVectors.get(i);
            context.doc().add(new FloatPoint(fieldName, projectedVector));
        }

        context.doc().addWithKey(fieldType().name(), field);
    }

    @Override
    protected void doXContentBody(XContentBuilder builder, boolean includeDefaults, Params params) throws IOException {
        super.doXContentBody(builder, includeDefaults, params);
        builder.field("dims", fieldType().dims());
        builder.field("top_hits", fieldType().topHits);
        builder.field("projections", fieldType().projections);
    }

    @Override
    protected void parseCreateField(ParseContext context, List<IndexableField> fields) {
        throw new AssertionError("parse is implemented directly");
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }
}
