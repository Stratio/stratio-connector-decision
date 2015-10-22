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

import com.stratio.crossdata.common.exceptions.ExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stratio.connector.commons.connection.Connection;
import com.stratio.connector.commons.util.PropertyValueRecovered;
import com.stratio.connector.decision.core.engine.query.util.StreamUtil;
import com.stratio.crossdata.common.connector.ConnectorClusterConfig;
import com.stratio.crossdata.common.exceptions.ConnectionException;
import com.stratio.crossdata.common.security.ICredentials;
import com.stratio.decision.api.IStratioStreamingAPI;
import com.stratio.decision.api.IStratioStreamingAPI;
import com.stratio.decision.api.StratioStreamingAPIFactory;
import com.stratio.decision.commons.exceptions.StratioEngineConnectionException;

/**
 * This class represents a logic connection to Decision.
 */
public class DecisionConnection extends Connection<IStratioStreamingAPI> {

    public static final String KAFKA_SERVER = "KafkaServer";
    public static final String KAFKA_PORT = "KafkaPort";
    public static final String ZOOKEEPER_SERVER = "zooKeeperServer";
    public static final String ZOOKEEPER_PORT = "zooKeeperPort";

    /**
     * The Log.
     */
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    /**
     * The Decision Connection.
     */
    private IStratioStreamingAPI stratioDecisionAPI = null;
    /**
     * The connection is connected.
     */
    private boolean isConnected = false;



    /**
     * Constructor.
     *
     * @param credentials
     *            the credentials.
     * @param config
     *            The cluster configuration.
     * @throws ConnectionException
     * @throws StratioEngineConnectionException
     *             if an connections error happens.
     */
    public DecisionConnection(ICredentials credentials, ConnectorClusterConfig config) throws ConnectionException, ExecutionException {

        if (credentials != null) {
            final String msg = "Credentials are not supported";
            logger.error(msg);
            throw new ConnectionException(msg);
        }

        String[] kafkaServer = PropertyValueRecovered.recoveredValueASArray(String.class, config.getClusterOptions().get(KAFKA_SERVER));
        String[] kafkaPort = PropertyValueRecovered.recoveredValueASArray(String.class, config.getClusterOptions().get(KAFKA_PORT));

        String[] zooKeeperServer = PropertyValueRecovered.recoveredValueASArray(String.class, config.getClusterOptions().get(ZOOKEEPER_SERVER));
        String[] zooKeeperPort = PropertyValueRecovered.recoveredValueASArray(String.class, config.getClusterOptions().get(ZOOKEEPER_PORT));

        if (kafkaServer.length != kafkaPort.length || zooKeeperServer.length != zooKeeperPort.length) {
            final String msg = "The number of hosts and ports must be equal";
            logger.error(msg);
            throw new ConnectionException(msg);
        }
        String kafkaQuorum = StreamUtil.getDecisionAddressFormat(kafkaServer, kafkaPort);
        String zookeeperQuorum = StreamUtil.getDecisionAddressFormat(zooKeeperServer, zooKeeperPort);

        stratioDecisionAPI = StratioStreamingAPIFactory.create().withServerConfig(kafkaQuorum, zookeeperQuorum);

        try {
            stratioDecisionAPI.init();
            // TODO is it async??
            logger.info("Decision  connection [" + config.getName().getName() + "] established ");
            isConnected = true;

        } catch (StratioEngineConnectionException e) {
            String msg = "Failure creating Decision connection. " + e.getMessage();
            logger.error(msg);
            throw new ConnectionException(msg, e);

        }

    }

    /**
     * This method close the connection.
     */
    public void close() {
        if (stratioDecisionAPI != null) {
            stratioDecisionAPI.close();
            isConnected = false;
            stratioDecisionAPI = null;
            logger.info("Decision  connection close");
        }

    }

    /**
     * Check if the connection is open.
     *
     * @return true if the connection is open. False in other case.
     */
    @Override
    public boolean isConnected() {
        return isConnected;
    }

    /**
     * Return a Decision native connection.
     *
     * @return a Decision native connection.
     */
    @Override
    public IStratioStreamingAPI getNativeConnection() {
        return stratioDecisionAPI;
    }

}
