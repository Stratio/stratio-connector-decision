/*
 * Licensed to STRATIO (C) under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information
 * regarding copyright ownership.  The STRATIO (C) licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.stratio.connector.decision.core.connection;

import com.stratio.connector.commons.TimerJ;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stratio.connector.commons.connection.Connection;
import com.stratio.connector.commons.connection.ConnectionHandler;
import com.stratio.crossdata.common.connector.ConnectorClusterConfig;
import com.stratio.crossdata.common.connector.IConfiguration;
import com.stratio.crossdata.common.exceptions.ConnectionException;
import com.stratio.crossdata.common.security.ICredentials;
import com.stratio.decision.api.IStratioStreamingAPI;

/**
 * This class manages the logic connections.
 */
public class DecisionConnectionHandler extends ConnectionHandler {

    /**
     * The Log.
     */
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    /**
     * constructor.
     *
     * @param configuration
     *            the configuration.
     */
    public DecisionConnectionHandler(IConfiguration configuration) {
        super(configuration);
    }

    /**
     * This method creates a steaming native connection.
     *
     * @param iCredentials
     *            the credentials.
     * @param connectorClusterConfig
     *            the configuration.
     * @return a logic connection with a native connection inside.
     * @throws ConnectionException
     *             if an error happens establishing the connection.
     */
    @Override
    @TimerJ
    protected Connection<IStratioStreamingAPI> createNativeConnection(ICredentials iCredentials,
                    ConnectorClusterConfig connectorClusterConfig) throws ConnectionException {
        Connection<IStratioStreamingAPI> con = null;
        try {
            con = new DecisionConnection(iCredentials, connectorClusterConfig);
        } catch (ExecutionException e) {
            throw new ConnectionException("An error ocurred during the connection");
        }

        return con;
    }

}
