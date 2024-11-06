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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.options.ConfigOption;
import org.apache.paimon.options.Options;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.paimon.options.ConfigOptions.key;

/** WriterConf for paimon. */
public class WriterConf implements Serializable {

    private static final long serialVersionUID = 1L;

    public static final ConfigOption<String> ALTER_SCHEMA =
            key("alter-schema")
                    .stringType()
                    .defaultValue("")
                    .withDescription(
                            "Sync table will alter schema with specified modes automatically.");

    public static final ConfigOption<String> IGNORE_SCHEMA_CHANGE =
            key("ignore-schema-change")
                    .stringType()
                    .defaultValue("")
                    .withDescription(
                            "It allows inconsistent fields to be written, you can add value like '<sourceType>-to-<schemaType>'."
                                    + " The following field types are currently compatible: "
                                    + " int-to-string ( this config including : bigint-to-string, smallint-to-string, int-to-string) .");

    private final Map<String, String> tableConf;
    private final Set<AlterSchemaMode> alterSchemaModes;
    private final Set<IgnoreSchemaChangeMode> ignoreSchemaChangeModes;

    public WriterConf(Options options) {
        this.tableConf = getTableConf(options);
        this.alterSchemaModes = getAlterSchemaModes(options);
        this.ignoreSchemaChangeModes = getIgnoreOptions(options);
    }

    public Set<IgnoreSchemaChangeMode> ignoreSchemaChangeModes() {
        return ignoreSchemaChangeModes;
    }

    protected Set<IgnoreSchemaChangeMode> getIgnoreOptions(Options options) {
        return IgnoreSchemaChangeMode.getIgnoreOptions(options);
    }

    public Map<String, String> tableConf() {
        return tableConf;
    }

    protected Map<String, String> getTableConf(Options options) {
        Map<String, String> conf = new HashMap<>();
        Set<String> coreOptionKeys = getCoreOptionKeys();
        Set<String> immutableCoreOptionKeys = CoreOptions.getImmutableOptionKeys();
        options.toMap()
                .forEach(
                        (k, v) -> {
                            if (v == null) {
                                conf.remove(k);
                            } else if (coreOptionKeys.contains(k)
                                    && (!immutableCoreOptionKeys.contains(k))) {
                                conf.put(k, v);
                            }
                        });
        return conf;
    }

    protected Set<String> getCoreOptionKeys() {
        return CoreOptions.getOptions().stream()
                .map(item -> item.key())
                .collect(Collectors.toSet());
    }

    public Set<AlterSchemaMode> alterSchemaModes() {
        return alterSchemaModes;
    }

    protected Set<AlterSchemaMode> getAlterSchemaModes(Options options) {
        if ("".equals(options.get(ALTER_SCHEMA))) {
            return Collections.emptySet();
        } else {
            String[] rawOptions = options.get(ALTER_SCHEMA).split(",");
            return Arrays.stream(rawOptions)
                    .map(String::trim)
                    .map(String::toLowerCase)
                    .map(AlterSchemaMode::mode)
                    .collect(Collectors.toSet());
        }
    }

    /** AlterSchemaMode for alter-schema. */
    public enum AlterSchemaMode {
        ADD_COLUMN,
        UPDATE_COLUMN;

        private static final Map<String, AlterSchemaMode> AlTER_SCHEMA_MODES =
                Arrays.stream(AlterSchemaMode.values())
                        .collect(
                                Collectors.toMap(
                                        AlterSchemaMode::configString, Function.identity()));

        public static AlterSchemaMode mode(String option) {
            AlterSchemaMode alterSchemaMode = AlTER_SCHEMA_MODES.get(option);
            if (alterSchemaMode == null) {
                throw new UnsupportedOperationException("Unsupported alter-schema mode: " + option);
            }
            return alterSchemaMode;
        }

        public String configString() {
            return name().toLowerCase().replace("_", "-");
        }
    }

    /** IgnoreSchemaChangeMode for ignore-schema-change. */
    public enum IgnoreSchemaChangeMode {
        INT_TO_STRING;

        private static final Map<String, IgnoreSchemaChangeMode> IGNORE_SCHEMA_CHANGE_MODES =
                Arrays.stream(IgnoreSchemaChangeMode.values())
                        .collect(
                                Collectors.toMap(
                                        IgnoreSchemaChangeMode::configString, Function.identity()));

        public static IgnoreSchemaChangeMode check(String option) {
            IgnoreSchemaChangeMode ignoreSchemaChangeMode = IGNORE_SCHEMA_CHANGE_MODES.get(option);
            if (ignoreSchemaChangeMode == null) {
                throw new UnsupportedOperationException(
                        "Unsupported ignore schema change option: " + option);
            }
            return ignoreSchemaChangeMode;
        }

        public static Set<IgnoreSchemaChangeMode> getIgnoreOptions(Options options) {
            Set<IgnoreSchemaChangeMode> modes = new HashSet<>();

            if (options.containsKey(IGNORE_SCHEMA_CHANGE.key())) {
                String[] split = options.getString(IGNORE_SCHEMA_CHANGE).split(",");
                modes =
                        Arrays.stream(split)
                                .map(String::trim)
                                .map(String::toLowerCase)
                                .map(IgnoreSchemaChangeMode::check)
                                .collect(Collectors.toSet());
            }
            return modes;
        }

        public String configString() {
            return name().toLowerCase().replace("_", "-");
        }
    }
}
