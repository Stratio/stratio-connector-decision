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

package com.stratio.connector.streaming.core.connection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stratio.connector.commons.connection.Connection;

import com.stratio.meta.common.connector.ConnectorClusterConfig;
import com.stratio.meta.common.security.ICredentials;
import com.stratio.streaming.api.IStratioStreamingAPI;
import com.stratio.streaming.api.StratioStreamingAPIFactory;
import com.stratio.streaming.commons.exceptions.StratioEngineConnectionException;

/**
 * This class represents a logic connection.
 * Created by jmgomez on 28/08/14.
 */
public class StreamingConnection extends Connection<IStratioStreamingAPI> {

    /**
     * The Log.
     */
    final Logger logger = LoggerFactory.getLogger(this.getClass());

    /**
     * The Streaming Connectio.
     */
    private IStratioStreamingAPI stratioStreamingAPI = null;
    /**
     * The connection is connected.
     */
    private boolean isConnect = false;

    private String connectionName;
    /**
     * Constructor.
     *
     * @param credentiasl the credentials.
     * @param config      The cluster configuration.
     */
    public StreamingConnection(ICredentials credentiasl, ConnectorClusterConfig config)
            throws StratioEngineConnectionException {

        stratioStreamingAPI = StratioStreamingAPIFactory.create().initializeWithServerConfig
                (config.getOptions().get("KafkaServer"), Integer.parseInt(config.getOptions().get("KafkaPort")),
                        config.getOptions().get("zooKeeperServer"),Integer.parseInt(config.getOptions().get
                                ("zooKeeperPort")));
        connectionName = config.getName().getName();
        logger.info("Streaming  connection ["+connectionName+"] established ");

        isConnect = true;
    }

    public void close() {
        if (stratioStreamingAPI != null) {
            isConnect = false;
            stratioStreamingAPI = null;
            logger.info("Streaming  connection ["+connectionName+"] close");
        }

    }

    @Override
    public boolean isConnect() {
        return isConnect;
    }

    @Override
    public IStratioStreamingAPI getNativeConnection() {
        return stratioStreamingAPI;
    }

}
