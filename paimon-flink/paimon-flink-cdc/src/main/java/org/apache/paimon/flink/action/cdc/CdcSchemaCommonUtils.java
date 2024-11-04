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

package org.apache.paimon.flink.action.cdc;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeChecks;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Common utils for Cdc Schema. */
public class CdcSchemaCommonUtils {
    private static final Logger LOG = LoggerFactory.getLogger(CdcSchemaCommonUtils.class);

    private static final List<DataTypeRoot> STRING_TYPES =
            Arrays.asList(DataTypeRoot.CHAR, DataTypeRoot.VARCHAR);
    private static final List<DataTypeRoot> BINARY_TYPES =
            Arrays.asList(DataTypeRoot.BINARY, DataTypeRoot.VARBINARY);
    private static final List<DataTypeRoot> INTEGER_TYPES =
            Arrays.asList(
                    DataTypeRoot.TINYINT,
                    DataTypeRoot.SMALLINT,
                    DataTypeRoot.INTEGER,
                    DataTypeRoot.BIGINT);
    private static final List<DataTypeRoot> FLOATING_POINT_TYPES =
            Arrays.asList(DataTypeRoot.FLOAT, DataTypeRoot.DOUBLE);

    private static final List<DataTypeRoot> DECIMAL_TYPES = Arrays.asList(DataTypeRoot.DECIMAL);

    private static final List<DataTypeRoot> TIMESTAMP_TYPES =
            Arrays.asList(DataTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE);

    public static boolean schemaCompatible(
            TableSchema paimonSchema,
            List<DataField> sourceTableFields,
            Set<WriterConf.IgnoreSchemaChangeMode> ignoreSchemaChangeModes) {
        for (DataField field : sourceTableFields) {
            int idx = paimonSchema.fieldNames().indexOf(field.name());
            if (idx < 0) {
                LOG.info("Cannot find field '{}' in Paimon table.", field.name());
                return false;
            }
            DataType type = paimonSchema.fields().get(idx).type();
            if (canConvert(type, field.type(), ignoreSchemaChangeModes)
                    == ConvertAction.EXCEPTION) {
                LOG.info(
                        "Cannot convert field '{}' from source table type '{}' to Paimon type '{}'.",
                        field.name(),
                        field.type(),
                        type);
                return false;
            }
        }
        return true;
    }

    public static List<SchemaChange> extractSchemaChanges(
            FileStoreTable oldTable,
            List<DataField> newDataFields,
            Set<WriterConf.AlterSchemaMode> alterSchemaModes) {
        RowType oldRowType = oldTable.schema().logicalRowType();
        Map<String, DataField> tmpOldFields = new HashMap<>();
        for (DataField oldField : oldRowType.getFields()) {
            tmpOldFields.put(oldField.name(), oldField);
        }

        List<SchemaChange> result = new ArrayList<>();
        for (DataField newField : newDataFields) {
            String newFieldName = newField.name();
            if (tmpOldFields.containsKey(newFieldName)) {
                if (alterSchemaModes.contains(WriterConf.AlterSchemaMode.UPDATE_COLUMN)) {
                    DataField oldField = tmpOldFields.get(newField.name());
                    // we compare by ignoring nullable, because partition keys and primary keys
                    // might be
                    // nullable in source database, but they can't be null in Paimon
                    if (oldField.type().equalsIgnoreNullable(newField.type())) {
                        // update column comment
                        if (newField.description() != null
                                && !newField.description().equals(oldField.description())) {
                            result.add(
                                    SchemaChange.updateColumnComment(
                                            new String[] {newField.name()},
                                            newField.description()));
                        }
                    } else {
                        // update column type
                        result.add(SchemaChange.updateColumnType(newField.name(), newField.type()));
                        // update column comment
                        if (newField.description() != null
                                && !newField.description().equals(oldField.description())) {
                            result.add(
                                    SchemaChange.updateColumnComment(
                                            new String[] {newField.name()},
                                            newField.description()));
                        }
                    }
                }

                tmpOldFields.remove(newFieldName);
            } else {
                if (alterSchemaModes.contains(WriterConf.AlterSchemaMode.ADD_COLUMN)) {
                    // add column
                    result.add(
                            SchemaChange.addColumn(
                                    newField.name(),
                                    newField.type(),
                                    newField.description(),
                                    null));
                }
            }
        }

        if (tmpOldFields.size() > 0) {
            LOG.warn(
                    "Cannot find Paimon fields '{}' in Source table '{}'.",
                    tmpOldFields,
                    oldTable.name());
            if (alterSchemaModes.contains(WriterConf.AlterSchemaMode.ADD_COLUMN)) {
                throw new IllegalArgumentException(
                        "Please check paimon table '"
                                + oldTable.name()
                                + "' fields '"
                                + tmpOldFields
                                + "' when use add-column mode, we can not find it in source table.");
            }
        }

        return result;
    }

    public static void applySchemaChange(
            TableSchema oldSchema,
            List<SchemaChange> schemaChanges,
            Identifier identifier,
            Catalog catalog,
            Set<WriterConf.IgnoreSchemaChangeMode> ignoreSchemaChangeModes) {
        if (schemaChanges == null || schemaChanges.size() == 0) {
            return;
        }
        List<SchemaChange> schemaChangesChecked = new ArrayList<>();
        for (SchemaChange schemaChange : schemaChanges) {
            if (schemaChange instanceof SchemaChange.AddColumn) {
                schemaChangesChecked.add(schemaChange);
            } else if (schemaChange instanceof SchemaChange.UpdateColumnType) {
                SchemaChange.UpdateColumnType updateColumnType =
                        (SchemaChange.UpdateColumnType) schemaChange;
                int idx = oldSchema.fieldNames().indexOf(updateColumnType.fieldName());
                Preconditions.checkState(
                        idx >= 0,
                        "Field name "
                                + updateColumnType.fieldName()
                                + " does not exist in table. This is unexpected.");
                DataType oldType = oldSchema.fields().get(idx).type();
                DataType newType = updateColumnType.newDataType();
                switch (canConvert(oldType, newType, ignoreSchemaChangeModes)) {
                    case CONVERT:
                        LOG.info(
                                "SchemaChange Update Field Type, convert field '{}' type from {} to {}.",
                                updateColumnType.fieldName(),
                                oldType,
                                newType);
                        schemaChangesChecked.add(schemaChange);
                        break;
                    case EXCEPTION:
                        throw new UnsupportedOperationException(
                                String.format(
                                        "Cannot convert field %s from type %s to %s of Paimon table %s.",
                                        updateColumnType.fieldName(),
                                        oldType,
                                        newType,
                                        identifier.getFullName()));
                }
            } else if (schemaChange instanceof SchemaChange.UpdateColumnComment) {
                SchemaChange.UpdateColumnComment updateColumnComment =
                        (SchemaChange.UpdateColumnComment) schemaChange;
                LOG.info(
                        "SchemaChange Update Field '{}' ColumnComment",
                        updateColumnComment.fieldNames());
                schemaChangesChecked.add(schemaChange);
            } else {
                throw new UnsupportedOperationException(
                        "Unsupported schema change class "
                                + schemaChange.getClass().getName()
                                + ", content "
                                + schemaChange);
            }
        }

        LOG.info("Schema will be changed, {} columns affected.", schemaChangesChecked.size());

        if (schemaChangesChecked.size() != 0) {
            try {
                catalog.alterTable(identifier, schemaChangesChecked, false);
            } catch (Catalog.TableNotExistException
                    | Catalog.ColumnAlreadyExistException
                    | Catalog.ColumnNotExistException e) {
                throw new RuntimeException("This is unexpected.", e);
            }
        }
    }

    public static ConvertAction canConvert(DataType oldType, DataType newType) {
        return canConvert(oldType, newType, Collections.emptySet());
    }

    public static ConvertAction canConvert(
            DataType oldType,
            DataType newType,
            Set<WriterConf.IgnoreSchemaChangeMode> ignoreSchemaChangeModes) {
        if (oldType.equalsIgnoreNullable(newType)) {
            return ConvertAction.CONVERT;
        }

        int oldIdx = STRING_TYPES.indexOf(oldType.getTypeRoot());
        int newIdx = STRING_TYPES.indexOf(newType.getTypeRoot());
        if (oldIdx >= 0 && newIdx >= 0) {
            return DataTypeChecks.getLength(oldType) <= DataTypeChecks.getLength(newType)
                    ? ConvertAction.CONVERT
                    : ConvertAction.IGNORE;
        }

        oldIdx = BINARY_TYPES.indexOf(oldType.getTypeRoot());
        newIdx = BINARY_TYPES.indexOf(newType.getTypeRoot());
        if (oldIdx >= 0 && newIdx >= 0) {
            return DataTypeChecks.getLength(oldType) <= DataTypeChecks.getLength(newType)
                    ? ConvertAction.CONVERT
                    : ConvertAction.IGNORE;
        }

        oldIdx = INTEGER_TYPES.indexOf(oldType.getTypeRoot());
        newIdx = INTEGER_TYPES.indexOf(newType.getTypeRoot());
        if (oldIdx >= 0 && newIdx >= 0) {
            return oldIdx <= newIdx ? ConvertAction.CONVERT : ConvertAction.IGNORE;
        }

        oldIdx = FLOATING_POINT_TYPES.indexOf(oldType.getTypeRoot());
        newIdx = FLOATING_POINT_TYPES.indexOf(newType.getTypeRoot());
        if (oldIdx >= 0 && newIdx >= 0) {
            return oldIdx <= newIdx ? ConvertAction.CONVERT : ConvertAction.IGNORE;
        }

        oldIdx = DECIMAL_TYPES.indexOf(oldType.getTypeRoot());
        newIdx = DECIMAL_TYPES.indexOf(newType.getTypeRoot());
        if (oldIdx >= 0 && newIdx >= 0) {
            return DataTypeChecks.getPrecision(newType) <= DataTypeChecks.getPrecision(oldType)
                            && DataTypeChecks.getScale(newType) <= DataTypeChecks.getScale(oldType)
                    ? ConvertAction.IGNORE
                    : ConvertAction.CONVERT;
        }

        oldIdx = TIMESTAMP_TYPES.indexOf(oldType.getTypeRoot());
        newIdx = TIMESTAMP_TYPES.indexOf(newType.getTypeRoot());
        if (oldIdx >= 0 && newIdx >= 0) {
            return DataTypeChecks.getPrecision(oldType) <= DataTypeChecks.getPrecision(newType)
                    ? ConvertAction.CONVERT
                    : ConvertAction.IGNORE;
        }

        if (ignoreSchemaChangeModes != null && !ignoreSchemaChangeModes.isEmpty()) {
            newIdx = INTEGER_TYPES.indexOf(newType.getTypeRoot());
            if (ignoreSchemaChangeModes.contains(WriterConf.IgnoreSchemaChangeMode.INT_TO_STRING)
                    && oldType.getTypeRoot() == DataTypeRoot.VARCHAR
                    && newIdx >= 0) {
                return ConvertAction.IGNORE;
            }
        }

        return ConvertAction.EXCEPTION;
    }

    /**
     * Return type of {@link CdcSchemaCommonUtils#canConvert(DataType, DataType)}. This enum
     * indicates the action to perform.
     */
    public enum ConvertAction {

        /** {@code oldType} can be converted to {@code newType}. */
        CONVERT,

        /**
         * {@code oldType} and {@code newType} belongs to the same type family, but old type has
         * higher precision than new type. Ignore this convert request.
         */
        IGNORE,

        /**
         * {@code oldType} and {@code newType} belongs to different type family. Throw an exception
         * indicating that this convert request cannot be handled.
         */
        EXCEPTION
    }
}
