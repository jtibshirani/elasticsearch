/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.vectors.query;

import org.apache.lucene.search.Query;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.xpack.vectors.query.VectorScoreQuery.Metric;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;

/**
 * A query that finds approximate nearest neighbours based on LSH hashes
 */
public class VectorScoreQueryBuilder extends AbstractQueryBuilder<VectorScoreQueryBuilder> {

    public static final String NAME = "vector_score";
    private static final ParseField FIELD_FIELD = new ParseField("field");
    private static final ParseField METRIC_FIELD = new ParseField("metric");
    private static final ParseField QUERY_VECTOR_FIELD = new ParseField("query_vector");

    private static ConstructingObjectParser<VectorScoreQueryBuilder, Void> PARSER = new ConstructingObjectParser<>(NAME, false,
        args -> {
            Metric metric = Metric.valueOf((String) args[1]);

            @SuppressWarnings("unchecked")
            List<Float> qvList = (List<Float>) args[2];
            float[] qv = new float[qvList.size()];
            int i = 0;
            for (Float f : qvList) {
                qv[i++] = f;
            };
            return new VectorScoreQueryBuilder((String) args[0], metric, qv);
        });

    static {
        PARSER.declareString(constructorArg(), FIELD_FIELD);
        PARSER.declareString(constructorArg(), METRIC_FIELD);
        PARSER.declareFloatArray(constructorArg(), QUERY_VECTOR_FIELD);
    }

    public static VectorScoreQueryBuilder fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    private final String field;
    private final Metric metric;
    private final float[] queryVector;

    public VectorScoreQueryBuilder(String field, Metric metric, float[] queryVector) {
        this.field = field;
        this.metric = metric;
        this.queryVector = queryVector;
    }

    public VectorScoreQueryBuilder(StreamInput in) throws IOException {
        super(in);
        field = in.readString();
        metric = in.readEnum(Metric.class);
        queryVector = in.readFloatArray();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(field);
        out.writeEnum(metric);
        out.writeFloatArray(queryVector);
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.field(FIELD_FIELD.getPreferredName(), field);
        builder.field(QUERY_VECTOR_FIELD.getPreferredName(), queryVector);
        printBoostAndQueryName(builder);
        builder.endObject();
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    protected boolean doEquals(VectorScoreQueryBuilder other) {
        return this.field.equals(other.field) &&
            this.metric.equals(other.metric) &&
            Arrays.equals(this.queryVector, other.queryVector);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(this.field, this.metric, this.queryVector);
    }

    @Override
    protected Query doToQuery(QueryShardContext context) {
        return new VectorScoreQuery(field, metric, queryVector);
    }
}
