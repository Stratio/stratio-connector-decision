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

package com.stratio.connector.streaming.core.connection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stratio.connector.commons.connection.Connection;
import com.stratio.connector.commons.util.ConnectorParser;
import com.stratio.connector.streaming.core.engine.query.util.StreamUtil;
import com.stratio.crossdata.common.connector.ConnectorClusterConfig;
import com.stratio.crossdata.common.exceptions.ConnectionException;
import com.stratio.crossdata.common.security.ICredentials;
import com.stratio.streaming.api.IStratioStreamingAPI;
import com.stratio.streaming.api.StratioStreamingAPIFactory;
import com.stratio.streaming.commons.exceptions.StratioEngineConnectionException;

/**
 * This class represents a logic connection to streaming.
 */
public class StreamingConnection extends Connection<IStratioStreamingAPI> {

    public static final String KAFKA_SERVER = "KafkaServer";
    public static final String KAFKA_PORT = "KafkaPort";
    public static final String ZOOKEEPER_SERVER = "zooKeeperServer";
    public static final String ZOOKEEPER_PORT = "zooKeeperPort";

    /**
     * The Log.
     */
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    /**
     * The Streaming Connection.
     */
    private IStratioStreamingAPI stratioStreamingAPI = null;
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
    public StreamingConnection(ICredentials credentials, ConnectorClusterConfig config) throws ConnectionException {

        if (credentials != null) {
            final String msg = "Credentials are not supported";
            logger.error(msg);
            throw new ConnectionException(msg);
        }

        String[] kafkaServer = ConnectorParser.hosts(config.getClusterOptions().get(KAFKA_SERVER));
        String[] kafkaPort = ConnectorParser.ports(config.getClusterOptions().get(KAFKA_PORT));

        String[] zooKeeperServer = ConnectorParser.hosts(config.getClusterOptions().get(ZOOKEEPER_SERVER));
        String[] zooKeeperPort = ConnectorParser.ports(config.getClusterOptions().get(ZOOKEEPER_PORT));

        if (kafkaServer.length != kafkaPort.length || zooKeeperServer.length != zooKeeperPort.length) {
            final String msg = "The number of hosts and ports must be equal";
            logger.error(msg);
            throw new ConnectionException(msg);
        }
        String kafkaQuorum = StreamUtil.getStreamingAddressFormat(kafkaServer, kafkaPort);
        String zookeeperQuorum = StreamUtil.getStreamingAddressFormat(zooKeeperServer, zooKeeperPort);

        stratioStreamingAPI = StratioStreamingAPIFactory.create().withServerConfig(kafkaQuorum, zookeeperQuorum);

        try {
            stratioStreamingAPI.init();
            // TODO is it async??
            logger.info("Streaming  connection [" + config.getName().getName() + "] established ");
            isConnected = true;

        } catch (StratioEngineConnectionException e) {
            String msg = "Failure creating Streaming connection. " + e.getMessage();
            logger.error(msg);
            throw new ConnectionException(msg, e);

        }

    }

    /**
     * This method close the connection.
     */
    public void close() {
        if (stratioStreamingAPI != null) {
            stratioStreamingAPI.close();
            isConnected = false;
            stratioStreamingAPI = null;
            logger.info("Streaming  connection close");
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
     * Return a streaming native connection.
     *
     * @return a streaming native connection.
     */
    @Override
    public IStratioStreamingAPI getNativeConnection() {
        return stratioStreamingAPI;
    }

}
