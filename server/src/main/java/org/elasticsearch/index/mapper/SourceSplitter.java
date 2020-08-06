/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class SourceSplitter {

    static byte[][] splitSchemaData(byte[] json) throws IOException {
        try (XContentParser parser = JsonXContent.jsonXContent.createParser(
            NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, json)) {
            BytesStreamOutput schema = new BytesStreamOutput();
            BytesStreamOutput data = new BytesStreamOutput();
            boolean prevIsValue = false;
            while (parser.nextToken() != null) {
                switch (parser.currentToken()) {
                    case START_OBJECT:
                        schema.writeByte((byte) '{');
                        break;
                    case END_OBJECT:
                        schema.writeByte((byte) '}');
                        break;
                    case FIELD_NAME:
                        schema.writeByte((byte) '"');
                        schema.writeBytes(parser.currentName().getBytes(StandardCharsets.UTF_8));
                        schema.writeByte((byte) '"');
                        prevIsValue = false;
                        break;
                    case START_ARRAY:
                        // Write to the data because we need the data to know when the array starts and ends anyway
                        data.writeByte((byte) '[');
                        break;
                    case END_ARRAY:
                        // Write to the data because we need the data to know when the array starts and ends anyway
                        data.writeByte((byte) ']');
                        break;
                    case VALUE_NULL:
                        if (prevIsValue) {
                            data.writeByte((byte) ',');
                        }
                        data.writeBytes("null".getBytes(StandardCharsets.UTF_8));
                        prevIsValue = true;
                        break;
                    case VALUE_STRING:
                        if (prevIsValue) {
                            data.writeByte((byte) ',');
                        }
                        data.writeByte((byte) '"');
                        data.writeBytes(parser.text().getBytes(StandardCharsets.UTF_8));
                        data.writeByte((byte) '"');
                        prevIsValue = true;
                        break;
                    case VALUE_BOOLEAN:
                    case VALUE_NUMBER:
                        if (prevIsValue) {
                            data.writeByte((byte) ',');
                        }
                        data.writeBytes(parser.text().getBytes(StandardCharsets.UTF_8));
                        prevIsValue = true;
                        break;
                    default:
                        throw new AssertionError();
                }
            }
            return new byte[][] { BytesReference.toBytes(schema.bytes()), BytesReference.toBytes(data.bytes()) };
        }
    }

}
