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

package com.stratio.connector.streaming.ftest;

import static org.mockito.Mockito.mock;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stratio.connector.streaming.core.StreamingConnector;
import com.stratio.connector.streaming.core.connection.StreamingConnection;
import com.stratio.crossdata.common.connector.ConnectorClusterConfig;
import com.stratio.crossdata.common.connector.IConfiguration;
import com.stratio.crossdata.common.data.ClusterName;
import com.stratio.crossdata.common.data.TableName;
import com.stratio.crossdata.common.exceptions.ConnectionException;
import com.stratio.crossdata.common.exceptions.ConnectorException;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.exceptions.InitializationException;
import com.stratio.crossdata.common.exceptions.UnsupportedException;
import com.stratio.crossdata.common.security.ICredentials;

/**
 * @author david
 *
 */
public abstract class GenericStreamingTest {

    protected String SERVER_KAFKA = "10.200.0.58";// "192.168.0.2";
    protected String PORT_KAFKA = "9092";
    protected String SERVER_ZOOKEEPER = "10.200.0.58"; // "192.168.0.2";
    protected String PORT_ZOOKEEPER = "2181";

    public static String STRING_COLUMN = "string_column";
    public static String INTEGER_COLUMN = "integer_column";
    public static String INTEGER_CHANGEABLE_COLUMN = "integer_changeable_column";
    public static String BOOLEAN_COLUMN = "boolean_column";
    public static String FLOAT_COLUMN = "float_column";
    public static String DOUBLE_COLUMN = "double_column";
    public static String LONG_COLUMN = "long_column";

    public String CATALOG = "catalog_functional_test";

    public final String TABLE = this.getClass().getSimpleName() + UUID.randomUUID().toString().replaceAll("-", "_");

    protected Random random;

    protected StreamingConnector sConnector;
    /**
     * The Log.
     */
    protected final Logger logger = LoggerFactory.getLogger(this.getClass());

    protected boolean deleteBeteweenTest = true;

    protected ClusterName getClusterName() {
        return new ClusterName(CATALOG + "_" + TABLE);
    }

    @Before
    public void setUp() throws ConnectorException {
        sConnector = new StreamingConnector();
        sConnector.init(getConfiguration());
        sConnector.connect(getICredentials(), getConnectorClusterConfig());
        random = new Random(new Date().getTime());
        try {
            deleteTable(CATALOG, TABLE);
        } catch (ExecutionException e) {
            logger.debug("The table did not exist");
        }
        // createTable

        logger.debug(CATALOG + "/" + TABLE);

    }

    protected void deleteTable(String catalog, String table) throws UnsupportedException, ExecutionException {
        try {
            if (deleteBeteweenTest) {
                sConnector.getMetadataEngine().dropTable(getClusterName(), new TableName(catalog, table));
            }
        } catch (Throwable t) {
            logger.error("Table does not exist");
        }
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
        return new ConnectorClusterConfig(getClusterName(), optionsNode);
    }

    protected ICredentials getICredentials() {
        return null;
    }

    @After
    public void tearDown() throws ConnectorException {

        if (deleteBeteweenTest) {
            deleteTable(CATALOG, TABLE);
            if (logger.isDebugEnabled()) {
                logger.debug("Delete Catalog: " + CATALOG);
                sConnector.close(getClusterName());
            }

        }
    }

}
