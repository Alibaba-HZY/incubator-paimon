/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.format.avro;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import java.util.function.Function;

/** Testing utils for tests related to {@link AbstractAvroBulkFormat}. */
public class AvroBulkFormatTestUtils {

    public static final RowType ROW_TYPE =
            (RowType)
                    RowType.builder()
                            .fields(
                                    new DataType[] {
                                        DataTypes.STRING(),
                                        DataTypes.STRING(),
                                        DataTypes.TIMESTAMP(3),
                                        DataTypes.TIMESTAMP(6),
                                        DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3),
                                        DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(6),
                                    },
                                    new String[] {
                                        "a",
                                        "b",
                                        "timestamp_millis",
                                        "timestamp_micros",
                                        "local_timestamp_millis",
                                        "local_timestamp_micros"
                                    })
                            .build()
                            .notNull();

    /** {@link AbstractAvroBulkFormat} for tests. */
    public static class TestingAvroBulkFormat extends AbstractAvroBulkFormat<GenericRecord> {

        protected TestingAvroBulkFormat() {
            super(AvroSchemaConverter.convertToSchema(ROW_TYPE));
        }

        @Override
        protected GenericRecord createReusedAvroRecord() {
            return new GenericData.Record(readerSchema);
        }

        @Override
        protected Function<GenericRecord, InternalRow> createConverter() {
            AvroToRowDataConverters.AvroToRowDataConverter converter =
                    AvroToRowDataConverters.createRowConverter(ROW_TYPE);
            return record -> record == null ? null : (InternalRow) converter.convert(record);
        }
    }
}
