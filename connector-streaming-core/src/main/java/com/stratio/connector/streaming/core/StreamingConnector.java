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

package com.stratio.connector.streaming.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stratio.connector.commons.CommonsConnector;
import com.stratio.connector.streaming.core.connection.StreamingConnectionHandler;
import com.stratio.connector.streaming.core.engine.StreamingMetadataEngine;
import com.stratio.connector.streaming.core.engine.StreamingQueryEngine;
import com.stratio.connector.streaming.core.engine.StreamingStorageEngine;
import com.stratio.connector.streaming.core.procces.ConnectorProcessHandler;
import com.stratio.connectors.ConnectorApp;
import com.stratio.meta.common.connector.IConfiguration;
import com.stratio.meta.common.connector.IMetadataEngine;
import com.stratio.meta.common.connector.IQueryEngine;
import com.stratio.meta.common.connector.IStorageEngine;
import com.stratio.meta.common.exceptions.ExecutionException;

/**
 * This class implements the connector for Streaming.
 */
public class StreamingConnector extends CommonsConnector {

    private transient ConnectorProcessHandler processHandler;

    /**
     * The Log.
     */
    final Logger logger = LoggerFactory.getLogger(this.getClass());

    /**
     * Create a connection to Streaming. The client will be a transportClient by default unless stratio nodeClient is
     * specified.
     *
     * @param configuration
     *            the connection configuration. It must be not null.
     */

    @Override
    public void init(IConfiguration configuration) {

        connectionHandler = new StreamingConnectionHandler(configuration);

        processHandler = new ConnectorProcessHandler();

    }

    @Override
    public String getConnectorName() {
        return "Streaming";
    }

    /**
     * Return the DataStore Name.
     *
     * @return DataStore Name
     */
    @Override
    public String[] getDatastoreName() {
        return new String[] { "Streaming" };
    }

    /**
     * Return the StorageEngine.
     *
     * @return the StorageEngine
     */
    @Override
    public IStorageEngine getStorageEngine() {

        return new StreamingStorageEngine(connectionHandler);

    }

    /**
     * Return the QueryEngine.
     *
     * @return the QueryEngine
     */
    @Override
    public IQueryEngine getQueryEngine() {

        return new StreamingQueryEngine(connectionHandler, processHandler);
    }

    /**
     * Return the MetadataEngine.
     *
     * @return the MetadataEngine
     */
    @Override
    public IMetadataEngine getMetadataEngine() {
        return new StreamingMetadataEngine(connectionHandler);
    }

    public static void main(String[] args) {
        StreamingConnector cassandraConnector = new StreamingConnector();
        ConnectorApp connectorApp = new ConnectorApp();
        connectorApp.startup(cassandraConnector);
        cassandraConnector.attachShutDownHook();
    }
    public void attachShutDownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                try {
                    shutdown();
                } catch (ExecutionException e) {
                    logger.error("Fail ShutDown");
                }
            }
        });
    }


}
