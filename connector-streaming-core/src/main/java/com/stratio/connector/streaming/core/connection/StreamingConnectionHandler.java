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

import com.stratio.connector.commons.connection.Connection;
import com.stratio.connector.commons.connection.ConnectionHandler;
import com.stratio.connector.elasticsearch.core.configuration.ConfigurationOptions;
import com.stratio.meta.common.connector.ConnectorClusterConfig;
import com.stratio.meta.common.connector.IConfiguration;
import com.stratio.meta.common.security.ICredentials;

/**
 * Created by jmgomez on 28/08/14.
 */
public class StreamingConnectionHandler extends ConnectionHandler {

    public StreamingConnectionHandler(IConfiguration configuration) {
        super(configuration);
    }

    @Override
    protected Connection createNativeConnection(ICredentials iCredentials,
            ConnectorClusterConfig connectorClusterConfig) {
        Connection connection;

        return new StreamingConnection();
    }

    private boolean isNodeClient(ConnectorClusterConfig config) {
        return Boolean.parseBoolean((String) config.getOptions().get(ConfigurationOptions.NODE_TYPE.getOptionName()));
    }

}
