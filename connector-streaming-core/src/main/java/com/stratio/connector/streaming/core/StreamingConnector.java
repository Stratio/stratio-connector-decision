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

import com.stratio.connector.commons.connection.exceptions.HandlerConnectionException;
import com.stratio.connector.elasticsearch.core.connection.ElasticSearchConnectionHandler;
import com.stratio.connector.elasticsearch.core.engine.ElasticsearchMetadataEngine;
import com.stratio.connector.elasticsearch.core.engine.ElasticsearchQueryEngine;
import com.stratio.connector.elasticsearch.core.engine.ElasticsearchStorageEngine;
import com.stratio.meta.common.connector.ConnectorClusterConfig;
import com.stratio.meta.common.connector.IConfiguration;
import com.stratio.meta.common.connector.IConnector;
import com.stratio.meta.common.connector.IMetadataEngine;
import com.stratio.meta.common.connector.IQueryEngine;
import com.stratio.meta.common.connector.IStorageEngine;
import com.stratio.meta.common.exceptions.ConnectionException;
import com.stratio.meta.common.exceptions.ExecutionException;
import com.stratio.meta.common.security.ICredentials;
import com.stratio.meta2.common.data.ClusterName;

/**
 * This class implements the connector for Elasticsearch.
 */
public class StreamingConnector implements IConnector {

    /**
     * The Log.
     */
    final Logger logger = LoggerFactory.getLogger(this.getClass());

    /**
     * The connectionHandler.
     */
    private ElasticSearchConnectionHandler connectionHandler = null;

    /**
     * Create a connection to Elasticsearch.
     * The client will be a transportClient by default unless stratio nodeClient is specified.
     *
     * @param configuration the connection configuration. It must be not null.
     *                      onnection.
     */

    @Override
    public void init(IConfiguration configuration) {

        connectionHandler = new ElasticSearchConnectionHandler(configuration);

    }

    /**
     * Create a connection with ElasticSearch.
     *
     * @param credentials the credentials.
     * @param config      the connection configuration.
     * @throws com.stratio.meta.common.exceptions.ConnectionException if the connection fail.
     */
    @Override
    public void connect(ICredentials credentials, ConnectorClusterConfig config) throws ConnectionException {
        try {
            connectionHandler.createConnection(credentials, config);
        } catch (HandlerConnectionException e) {
            String msg = "fail creating the Connection. " + e.getMessage();
            logger.error(msg);
            throw new ConnectionException(msg, e);
        }
    }

    /**
     * It close the  connection to ElasticSearch.
     *
     * @param name the connection identifier.
     */
    @Override
    public void close(ClusterName name) {
        connectionHandler.closeConnection(name.getName());

    }

    @Override
    public void shutdown() throws ExecutionException {

    }

    @Override
    public String getConnectorName() {
        return "ElasticSearch";
    }

    /**
     * Return the DataStore Name.
     *
     * @return DataStore Name
     */
    @Override
    public String[] getDatastoreName() {
        return new String[] { "ElasticSearch" };
    }

    /**
     * Return the StorageEngine.
     *
     * @return the StorageEngine
     */
    @Override
    public IStorageEngine getStorageEngine() {

        return new ElasticsearchStorageEngine(connectionHandler);

    }

    /**
     * Return the QueryEngine.
     *
     * @return the QueryEngine
     */
    @Override
    public IQueryEngine getQueryEngine() {

        return new ElasticsearchQueryEngine(connectionHandler);
    }

    /**
     * Return the MetadataEngine.
     *
     * @return the MetadataEngine
     */
    @Override
    public IMetadataEngine getMetadataEngine() {
        return new ElasticsearchMetadataEngine(connectionHandler);
    }

    /**
     * The connection status.
     *
     * @param name the cluster Name.
     * @return true if the driver's client is not null.
     */
    @Override
    public boolean isConnected(ClusterName name) {

        return connectionHandler.isConnected(name.getName());

    }

}
