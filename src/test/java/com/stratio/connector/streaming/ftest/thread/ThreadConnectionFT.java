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

package com.stratio.connector.streaming.ftest.thread;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stratio.connector.streaming.core.StreamingConnector;
import com.stratio.connector.streaming.core.connection.StreamingConnection;
import com.stratio.crossdata.common.connector.ConnectorClusterConfig;
import com.stratio.crossdata.common.connector.IConfiguration;
import com.stratio.crossdata.common.data.ClusterName;
import com.stratio.crossdata.common.exceptions.ConnectorException;
import com.stratio.crossdata.common.security.ICredentials;
import com.stratio.streaming.api.StratioStreamingAPI;
import com.stratio.streaming.api.StratioStreamingAPIFactory;
import com.stratio.streaming.commons.exceptions.StratioEngineConnectionException;

public class ThreadConnectionFT {

    public final String TABLE = this.getClass().getSimpleName() + UUID.randomUUID().toString().replaceAll("-", "_");
    /**
     * The Log.
     */
    protected final Logger logger = LoggerFactory.getLogger(this.getClass());
    public String CATALOG = "catalog_functional_test";
    protected String SERVER_KAFKA = "127.0.0.1";// "10.200.0.58";// "192.168.0.2";
    protected String PORT_KAFKA = "9092";
    protected String SERVER_ZOOKEEPER = "127.0.0.1"; // "192.168.0.2";
    protected String PORT_ZOOKEEPER = "2181";
    protected Random random;
    protected StreamingConnector sConnector;
    protected boolean deleteBeteweenTest = true;

    protected ClusterName getClusterName() {
        return new ClusterName(CATALOG + "_" + TABLE);
    }

    @Test
    public void connectTest() throws ConnectorException {
        sConnector = new StreamingConnector();
        sConnector.init(getConfiguration());
        sConnector.connect(getICredentials(), getConnectorClusterConfig());

        waitSeconds(5);

        sConnector.close(getClusterName());

        waitSeconds(5);
        sConnector = new StreamingConnector();
        sConnector.init(getConfiguration());
        sConnector.connect(getICredentials(), getConnectorClusterConfig());

        waitSeconds(5);

        sConnector.close(getClusterName());

        assertTrue(true);

    }

    @Test
    public void apiStreamingTest() throws ConnectorException, StratioEngineConnectionException {

        StratioStreamingAPI stratioStreamingAPI = (StratioStreamingAPI) StratioStreamingAPIFactory.create()
                        .withServerConfig("127.0.0.1", 9092, "127.0.0.1", 2182).init();

        waitSeconds(5);

        stratioStreamingAPI.close();

        waitSeconds(5);

        StratioStreamingAPI stratioStreamingAPI2 = (StratioStreamingAPI) StratioStreamingAPIFactory.create()
                        .withServerConfig("127.0.0.1", 9092, "127.0.0.1", 2181).init();

        waitSeconds(5);

        stratioStreamingAPI2.close();

        assertTrue(true);

    }

    protected void waitSeconds(int sec) {
        try {
            Thread.sleep(sec * 1000);
        } catch (InterruptedException e) {
            logger.error("A thread has been interrupted unexpectedly");
            e.printStackTrace();
        }
    }

    protected IConfiguration getConfiguration() {
        return mock(IConfiguration.class);
    }

    protected ConnectorClusterConfig getConnectorClusterConfig() {
        Map<String, String> optionsNode = new HashMap<>();
        optionsNode.put(StreamingConnection.KAFKA_SERVER, SERVER_KAFKA);
        optionsNode.put(StreamingConnection.KAFKA_PORT, PORT_KAFKA);
        optionsNode.put(StreamingConnection.ZOOKEEPER_SERVER, SERVER_ZOOKEEPER);
        optionsNode.put(StreamingConnection.ZOOKEEPER_PORT, PORT_ZOOKEEPER);
        return new ConnectorClusterConfig(getClusterName(), null, optionsNode);
    }

    protected ICredentials getICredentials() {
        return null;
    }

}
