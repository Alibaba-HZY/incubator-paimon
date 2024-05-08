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

import org.apache.paimon.hive.mapred.PaimonInputFormat;
import org.apache.paimon.hive.mapred.PaimonOutputCommitter;
import org.apache.paimon.hive.mapred.PaimonOutputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.ql.metadata.HiveStoragePredicateHandler;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.security.authorization.HiveAuthorizationProvider;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Properties;

import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVEQUERYSTRING;
import static org.apache.paimon.utils.PartitionPathUtils.extractIsOverwriteFromSql;

/** {@link HiveStorageHandler} for paimon. This is the entrance class of Hive API. */
public class PaimonStorageHandler implements HiveStoragePredicateHandler, HiveStorageHandler {

    private static final Logger LOG = LoggerFactory.getLogger(PaimonStorageHandler.class);

    private static final String MAPRED_OUTPUT_COMMITTER = "mapred.output.committer.class";
    private static final String PAIMON_WRITE = "paimon.write";
    public static final String IS_OVERWRITE = "paimon.mr.write.is.overwrite";
    public static final String INSERT_OVERWRITE = "insert overwrite";

    private Configuration conf;

    @Override
    public Class<? extends InputFormat> getInputFormatClass() {
        return PaimonInputFormat.class;
    }

    @Override
    public Class<? extends OutputFormat> getOutputFormatClass() {
        return PaimonOutputFormat.class;
    }

    @Override
    public Class<? extends AbstractSerDe> getSerDeClass() {
        return PaimonSerDe.class;
    }

    @Override
    public HiveMetaHook getMetaHook() {
        return new PaimonMetaHook(this.conf);
    }

    @Override
    public HiveAuthorizationProvider getAuthorizationProvider() throws HiveException {
        return null;
    }

    @Override
    public void configureInputJobProperties(TableDesc tableDesc, Map<String, String> map) {
        Properties properties = tableDesc.getProperties();
        map.put(
                LocationKeyExtractor.INTERNAL_LOCATION,
                LocationKeyExtractor.getPaimonLocation(conf, properties));
    }

    public void configureInputJobCredentials(TableDesc tableDesc, Map<String, String> map) {}

    @Override
    public void configureOutputJobProperties(TableDesc tableDesc, Map<String, String> map) {
        Properties properties = tableDesc.getProperties();
        map.put(
                LocationKeyExtractor.INTERNAL_LOCATION,
                LocationKeyExtractor.getPaimonLocation(conf, properties));
        map.put(MAPRED_OUTPUT_COMMITTER, PaimonOutputCommitter.class.getName());
        map.put(PAIMON_WRITE, Boolean.TRUE.toString());
        properties.put(PAIMON_WRITE, Boolean.TRUE.toString());
    }

    @Override
    public void configureTableJobProperties(TableDesc tableDesc, Map<String, String> map) {}

    @Override
    public void configureJobConf(TableDesc tableDesc, JobConf jobConf) {
        if (tableDesc != null
                && tableDesc.getProperties() != null
                && tableDesc.getProperties().get(PAIMON_WRITE) != null) {

            jobConf.set(MAPRED_OUTPUT_COMMITTER, PaimonOutputCommitter.class.getName());
            try {
                String sql =
                        URLDecoder.decode(
                                jobConf.get(HIVEQUERYSTRING.varname),
                                StandardCharsets.UTF_8.name());
                jobConf.setBoolean(IS_OVERWRITE, extractIsOverwriteFromSql(sql));
            } catch (UnsupportedEncodingException e) {
                LOG.error("Decode hive.query.string sql error :", e);
            }
        }
    }

    @Override
    public void setConf(Configuration configuration) {
        this.conf = configuration;
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    @Override
    public DecomposedPredicate decomposePredicate(
            JobConf jobConf, Deserializer deserializer, ExprNodeDesc predicate) {
        DecomposedPredicate decomposed = new DecomposedPredicate();
        decomposed.residualPredicate = (ExprNodeGenericFuncDesc) predicate;
        decomposed.pushedPredicate = (ExprNodeGenericFuncDesc) predicate;
        return decomposed;
    }
}
