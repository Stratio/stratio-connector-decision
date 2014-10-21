/*
 * Licensed to STRATIO (C) under one or more contributor license agreements.
 *  See the NOTICE file distributed with this work for additional information
 *  regarding copyright ownership. The STRATIO (C) licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License. You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied. See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
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
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

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
