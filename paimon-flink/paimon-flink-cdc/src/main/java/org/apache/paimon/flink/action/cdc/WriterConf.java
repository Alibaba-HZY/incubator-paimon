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

import org.apache.paimon.options.ConfigOption;
import org.apache.paimon.options.Options;

import java.io.Serializable;

import static org.apache.paimon.options.ConfigOptions.key;

/** WriterConf for paimon. */
public class WriterConf implements Serializable {

    private static final long serialVersionUID = 1L;

    public static final ConfigOption<Boolean> ALTER_SCHEMA_WITH_ADD_COLUMN =
            key("alter-schema-with-add-column")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "If set to true, Sync table will alter schema with add column automatically.");

    private final Options options;

    public WriterConf(Options options) {
        this.options = options;
    }

    public boolean alterSchemaWithAddColumn() {
        return options.get(ALTER_SCHEMA_WITH_ADD_COLUMN);
    }
}