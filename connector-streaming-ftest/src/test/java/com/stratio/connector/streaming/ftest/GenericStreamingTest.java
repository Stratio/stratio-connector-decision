/*
 * Stratio Meta
 *
 *   Copyright (c) 2014, Stratio, All rights reserved.
 *
 *   This library is free software; you can redistribute it and/or modify it under the terms of the
 *   GNU Lesser General Public License as published by the Free Software Foundation; either version
 *   3.0 of the License, or (at your option) any later version.
 *
 *   This library is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 *   even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 *   Lesser General Public License for more details.
 *
 *   You should have received a copy of the GNU Lesser General Public License along with this library.
 */

package com.stratio.connector.streaming.ftest;

import static org.mockito.Mockito.mock;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stratio.connector.streaming.core.StreamingConnector;
import com.stratio.connector.streaming.core.connection.StreamingConnection;
import com.stratio.meta.common.connector.ConnectorClusterConfig;
import com.stratio.meta.common.connector.IConfiguration;
import com.stratio.meta.common.exceptions.ConnectionException;
import com.stratio.meta.common.exceptions.ExecutionException;
import com.stratio.meta.common.exceptions.InitializationException;
import com.stratio.meta.common.exceptions.UnsupportedException;
import com.stratio.meta.common.security.ICredentials;
import com.stratio.meta2.common.data.ClusterName;
import com.stratio.meta2.common.data.TableName;

/**
 * @author david
 *
 */
public abstract class GenericStreamingTest {

    protected String SERVER_KAFKA = "10.200.0.58";
    protected String PORT_KAFKA = "9092";
    protected String SERVER_ZOOKEEPER = "10.200.0.58";
    protected String PORT_ZOOKEEPER = "2181";

    public static String STRING_COLUMN = "string_column";
    public static String INTEGER_COLUMN = "integer_column";
    public static String BOOLEAN_COLUMN = "boolean_column";
    public static String FLOAT_COLUMN = "float_column";
    public static String DOUBLE_COLUMN = "double_column";
    public static String LONG_COLUMN = "long_column";

    public String CATALOG = "catalog_functional_test";
    public final String TABLE = this.getClass().getSimpleName() + UUID.randomUUID();

    protected StreamingConnector sConnector;
    /**
     * The Log.
     */
    protected final Logger logger = LoggerFactory.getLogger(this.getClass());

    protected boolean deleteBeteweenTest = true;

    protected ClusterName getClusterName() {
        return new ClusterName(CATALOG + "-" + TABLE);
    }

    @Before
    public void setUp() throws InitializationException, ConnectionException, UnsupportedException, ExecutionException {
        sConnector = new StreamingConnector();
        sConnector.init(getConfiguration());
        sConnector.connect(getICredentials(), getConnectorClusterConfig());

        try {
            deleteTable(CATALOG, TABLE);
        } catch (ExecutionException e) {
            logger.debug("The table did not exist");
        }

        // createTable

        logger.debug(CATALOG + "/" + TABLE);
    }

    // protected createTable

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
    public void tearDown() throws ConnectionException, UnsupportedException, ExecutionException {

        if (deleteBeteweenTest) {
            deleteTable(CATALOG, TABLE);
            if (logger.isDebugEnabled()) {
                logger.debug("Delete Catalog: " + CATALOG);
                sConnector.close(getClusterName());
            }

        }
    }

}
