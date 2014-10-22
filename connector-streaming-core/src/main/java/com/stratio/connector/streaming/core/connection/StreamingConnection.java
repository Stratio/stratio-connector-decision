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
import com.stratio.connector.commons.util.ConnectorParser;
import com.stratio.crossdata.common.connector.ConnectorClusterConfig;
import com.stratio.crossdata.common.exceptions.UnsupportedException;
import com.stratio.crossdata.common.security.ICredentials;
import com.stratio.streaming.api.IStratioStreamingAPI;
import com.stratio.streaming.api.StratioStreamingAPIFactory;
import com.stratio.streaming.commons.exceptions.StratioEngineConnectionException;

/**
 * This class represents a logic connection. Created by jmgomez on 28/08/14.
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
    private boolean isConnect = false;

    private String connectionName;

    /**
     * Constructor.
     *
     * @param credentiasl
     *            the credentials.
     * @param config
     *            The cluster configuration.
     * @throws UnsupportedException
     */
    public StreamingConnection(ICredentials credentials, ConnectorClusterConfig config)
                    throws StratioEngineConnectionException {

        if (credentials != null) {
            throw new StratioEngineConnectionException("Credentials are not supported");
        }

        String kafkaServer = ConnectorParser.hosts(config.getOptions().get(KAFKA_SERVER))[0];
        int kafkaPort = Integer.parseInt(ConnectorParser.ports(config.getOptions().get(KAFKA_PORT))[0]);

        String zooKeeperServer = ConnectorParser.hosts(config.getOptions().get(ZOOKEEPER_SERVER))[0];
        int zooKeeperPort = Integer.parseInt(ConnectorParser.ports(config.getOptions().get(ZOOKEEPER_PORT))[0]);

        stratioStreamingAPI = StratioStreamingAPIFactory.create().initializeWithServerConfig(kafkaServer, kafkaPort,
                        zooKeeperServer, zooKeeperPort);

        connectionName = config.getName().getName();
        logger.info("Streaming  connection [" + connectionName + "] established ");

        isConnect = true;
    }

    public void close() {
        if (stratioStreamingAPI != null) {
            isConnect = false;
            stratioStreamingAPI = null;
            logger.info("Streaming  connection [" + connectionName + "] close");
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
