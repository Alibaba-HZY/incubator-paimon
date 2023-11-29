/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.hive;

import org.apache.paimon.hive.objectinspector.PaimonInternalRowObjectInspector;

import org.apache.paimon.shade.guava30.com.google.common.collect.Maps;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.SubStructObjectInspector;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.UnsupportedEncodingException;
import java.lang.reflect.Field;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVEQUERYSTRING;
import static org.apache.paimon.utils.PartitionPathUtils.extractDynamicPartitionSpecFromQuery;

/**
 * {@link AbstractSerDe} for paimon. It transforms map-reduce values to Hive objects.
 *
 * <p>Currently this class only supports deserialization.
 */
public class PaimonSerDe extends AbstractSerDe {

    private static final Logger LOG = LoggerFactory.getLogger(PaimonSerDe.class);

    private PaimonInternalRowObjectInspector inspector;
    private PaimonInternalRowObjectInspector inspectorWithPartition;
    private HiveSchema tableSchema;
    private HiveSchema tableSchemaWithPartition;

    private final RowDataContainer rowData = new RowDataContainer();

    private final Map<ObjectInspector, HiveDeserializer> deserializers =
            Maps.newHashMapWithExpectedSize(1);
    private final Map<HiveSchema, LinkedHashMap<String, String>> fullPartSpecs =
            Maps.newHashMapWithExpectedSize(1);

    @Override
    public void initialize(@Nullable Configuration configuration, Properties properties)
            throws SerDeException {
        HiveSchema schema = HiveSchema.extract(configuration, properties);
        HiveSchema schemaWithPartition = HiveSchema.extract(configuration, properties, true);
        this.tableSchema = schema;
        this.tableSchemaWithPartition = schemaWithPartition;
        inspector =
                new PaimonInternalRowObjectInspector(
                        schema.fieldNames(), schema.fieldTypes(), schema.fieldComments());
        inspectorWithPartition =
                new PaimonInternalRowObjectInspector(
                        schemaWithPartition.fieldNames(),
                        schemaWithPartition.fieldTypes(),
                        schemaWithPartition.fieldComments());
        LinkedHashMap<String, String> fullPartSpec = fullPartSpecs.get(schema);
        if (fullPartSpec == null) {
            try {
                String sql =
                        URLDecoder.decode(
                                configuration.get(HIVEQUERYSTRING.varname),
                                StandardCharsets.UTF_8.name());
                fullPartSpec =
                        extractDynamicPartitionSpecFromQuery(
                                URLDecoder.decode(sql, StandardCharsets.UTF_8.name()));
                fullPartSpecs.put(schemaWithPartition, fullPartSpec);
            } catch (UnsupportedEncodingException e) {
                LOG.error("Decode hive.query.string sql error :", e);
            }
        }
    }

    @Override
    public Class<? extends Writable> getSerializedClass() {
        return RowDataContainer.class;
    }

    @Override
    public Writable serialize(Object o, ObjectInspector objectInspector) throws SerDeException {
        HiveDeserializer deserializer = deserializers.get(objectInspector);
        if (deserializer == null) {
            StructObjectInspector structObjectInspector = (StructObjectInspector) objectInspector;
            ObjectInspector richObjectInspector = objectInspector;
            if (structObjectInspector instanceof SubStructObjectInspector) {
                SubStructObjectInspector subStructObjectInspector =
                        (SubStructObjectInspector) structObjectInspector;
                Class<?> cls = subStructObjectInspector.getClass();
                Field field = null;
                try {
                    field = cls.getDeclaredField("baseOI");
                    field.setAccessible(true);

                    StructObjectInspector baseOI =
                            (StructObjectInspector) field.get(subStructObjectInspector);
                    richObjectInspector =
                            new SubStructObjectInspector(
                                    baseOI, 0, baseOI.getAllStructFieldRefs().size());
                } catch (NoSuchFieldException | IllegalAccessException e) {
                    throw new RuntimeException(e);
                }
            }
            deserializer =
                    new HiveDeserializer.Builder()
                            .schema(tableSchemaWithPartition)
                            .sourceInspector((StructObjectInspector) richObjectInspector)
                            .writerInspector(inspectorWithPartition)
                            .fullPartSpec(fullPartSpecs.get(tableSchemaWithPartition))
                            .build();
            deserializers.put(objectInspector, deserializer);
        }

        rowData.set(deserializer.deserialize(o));
        return rowData;
    }

    @Override
    public SerDeStats getSerDeStats() {
        return null;
    }

    @Override
    public Object deserialize(Writable writable) throws SerDeException {
        return ((RowDataContainer) writable).get();
    }

    @Override
    public ObjectInspector getObjectInspector() throws SerDeException {
        return inspector;
    }
}
