/*
 * Licensed to STRATIO (C) under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information
 * regarding copyright ownership. The STRATIO (C) licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.stratio.connector.decision.ftest.helper;

import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.Assert;

import com.stratio.connector.commons.ftest.helper.IConnectorHelper;
import com.stratio.connector.decision.core.DecisionConnector;
import com.stratio.crossdata.common.connector.ConnectorClusterConfig;
import com.stratio.crossdata.common.connector.IConfiguration;
import com.stratio.crossdata.common.connector.IConnector;
import com.stratio.crossdata.common.data.ClusterName;
import com.stratio.crossdata.common.exceptions.ConnectionException;
import com.stratio.crossdata.common.exceptions.InitializationException;
import com.stratio.crossdata.common.metadata.ColumnType;
import com.stratio.crossdata.common.metadata.DataType;
import com.stratio.crossdata.common.security.ICredentials;

public class DecisionConnectorHelper implements IConnectorHelper {

    private static final String ZOOKEEPER_PORT = "2181";
    private static final String ZOOKEEPER_SERVER =  "10.200.0.58"; // "192.168.0.2";
    protected String KAFKA_SERVER = "10.200.0.58";// "192.168.0.2"; //;
    private String KAFKA_PORT = "9092";
    private ClusterName clusterName;

    public DecisionConnectorHelper(ClusterName clusterName) throws ConnectionException, InitializationException {
        super();
        this.clusterName = clusterName;
    }

    @Override
    public IConnector getConnector() {
        try {
            return new DecisionConnector();
        } catch (InitializationException e) {
            // TODO Auto-generated catch block
            Assert.fail("Cannot retrieve the connector");
            return null;
        }

    }

    @Override
    public IConfiguration getConfiguration() {
        return mock(IConfiguration.class);
    }

    @Override
    public ConnectorClusterConfig getConnectorClusterConfig() {
        Map<String, String> optionsNode = new HashMap<>();

        optionsNode.put("KafkaServer", KAFKA_SERVER);
        optionsNode.put("KafkaPort", KAFKA_PORT);
        optionsNode.put("zooKeeperServer", ZOOKEEPER_SERVER);
        optionsNode.put("zooKeeperPort", ZOOKEEPER_PORT);

        return new ConnectorClusterConfig(clusterName, null, optionsNode);
    }

    @Override
    public ICredentials getICredentials() {
        return null;
        // TODO mock(ICredentials.class);
    }

    @Override
    public Map<String, Object> recoveredCatalogSettings(String indexName) {

        Map<String, Object> result = new HashMap<>();

        fail("Not yet Decision supported");
        return result;
    }

    @Override
    public Collection<ColumnType> getAllSupportedColumnType() {
        Set<ColumnType> allColumntTypes = new HashSet<>();

        allColumntTypes.add(new ColumnType(DataType.BIGINT));
        allColumntTypes.add(new ColumnType(DataType.BOOLEAN));
        allColumntTypes.add(new ColumnType(DataType.DOUBLE));
        allColumntTypes.add(new ColumnType(DataType.FLOAT));
        allColumntTypes.add(new ColumnType(DataType.INT));
        allColumntTypes.add(new ColumnType(DataType.TEXT));
        allColumntTypes.add(new ColumnType(DataType.VARCHAR));
        return allColumntTypes;
    }

    @Override
    public boolean containsIndex(String catalogName, String collectionName, String indexName) {
        fail("Not yet Decision supported");
        return false;
    }

    @Override
    public int countIndexes(String catalogName, String collectionName) {
        fail("Not yet Decision supported");
        return 0;
    }

    @Override
    public void refresh(String schema) {

    }

    @Override
    public boolean isCatalogMandatory() {

        return false;
    }

    @Override
    public boolean isTableMandatory() {
        return true;
    }

    @Override
    public boolean isIndexMandatory() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean isPKMandatory() {
        // TODO Auto-generated method stub
        return false;
    }

}
