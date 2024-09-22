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

import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.utils.Preconditions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Utility methods for {@link ComputedColumn}, such as build. */
public class ExtraColumnUtils {

    public static List<ExtraColumn> buildExtraColumns(
            List<String> computedColumnArgs, List<DataField> physicFields) {
        return buildExtraColumns(computedColumnArgs, physicFields, true);
    }

    /** The caseSensitive only affects check. We don't change field names at building phase. */
    public static List<ExtraColumn> buildExtraColumns(
            List<String> computedColumnArgs, List<DataField> physicFields, boolean caseSensitive) {
        Map<String, DataType> typeMapping =
                physicFields.stream()
                        .collect(
                                Collectors.toMap(DataField::name, DataField::type, (v1, v2) -> v2));

        List<ExtraColumn> computedColumns = new ArrayList<>();
        for (String columnArg : computedColumnArgs) {
            String[] kv = columnArg.split("=");
            if (kv.length != 2) {
                throw new IllegalArgumentException(
                        String.format(
                                "Invalid computed column argument: %s. Please use format 'column-name=expr-name(args, ...)'.",
                                columnArg));
            }
            String columnName = kv[0].trim();
            String expression = kv[1].trim();
            // parse expression
            int left = expression.indexOf('(');
            int right = expression.indexOf(')');
            Preconditions.checkArgument(
                    left > 0 && right > left,
                    String.format(
                            "Invalid expression: %s. Please use format 'expr-name(args, ...)'.",
                            expression));

            String exprName = expression.substring(0, left);
            String[] args = expression.substring(left + 1, right).split(",");
            checkArgument(args.length >= 1, "Computed column needs at least one argument.");
            String defaultValue = args[0].trim();
            String[] literals =
                    Arrays.stream(args).skip(1).map(String::trim).toArray(String[]::new);

            computedColumns.add(
                    new ExtraColumn(
                            columnName,
                            NoFiledExpression.create(exprName, defaultValue, literals),
                            createExtraDescription(columnName)));
        }

        return computedColumns;
    }

    public static String createExtraDescription(String columnName) {
        switch (columnName.toUpperCase()) {
            case "PROV":
                return "省分";
            case "SYSTEM_OP_TS":
                return "CDC消息时间";
            case "SYSTEM_OP_ID":
                return "CDC消息ID";
            case "SYSTEM_PHYSICAL_DB":
                return "物理库,分库分表下对应物理实例,非分库分表可不添加,以逻辑库名称标识";
            case "SYSTEM_PHYSICAL_TABLE":
                return "物理表,分库分表下对应物理实例,非分库分表可不添加,以逻辑表名称标识";
            default:
                break;
        }
        return "";
    }
}
