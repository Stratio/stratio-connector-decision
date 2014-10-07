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
import com.stratio.connector.commons.connection.ConnectionHandler;
import com.stratio.connector.commons.connection.exceptions.CreateNativeConnectionException;
import com.stratio.meta.common.connector.ConnectorClusterConfig;
import com.stratio.meta.common.connector.IConfiguration;
import com.stratio.meta.common.security.ICredentials;
import com.stratio.streaming.api.IStratioStreamingAPI;
import com.stratio.streaming.commons.exceptions.StratioEngineConnectionException;

/**
 * Created by jmgomez on 28/08/14.
 */
public class StreamingConnectionHandler extends ConnectionHandler {

    /**
     * The Log.
     */
    final Logger logger = LoggerFactory.getLogger(this.getClass());

    public StreamingConnectionHandler(IConfiguration configuration) {
        super(configuration);
    }

    @Override
    protected Connection<IStratioStreamingAPI> createNativeConnection(ICredentials iCredentials,
                    ConnectorClusterConfig connectorClusterConfig) throws CreateNativeConnectionException {
        Connection<IStratioStreamingAPI> con = null;
        try {
            con = new StreamingConnection(iCredentials, connectorClusterConfig);
        } catch (StratioEngineConnectionException e) {
            String msg = "Fail creating Streaming connection. " + e.getMessage();
            logger.error(msg);
            throw new CreateNativeConnectionException(msg, e);
        }
        return con;
    }

}
