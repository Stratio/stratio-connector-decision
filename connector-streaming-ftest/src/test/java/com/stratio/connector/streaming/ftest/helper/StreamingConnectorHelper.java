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

package com.stratio.connector.streaming.ftest.helper;

import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.stratio.connector.commons.ftest.helper.IConnectorHelper;
import com.stratio.connector.streaming.core.StreamingConnector;
import com.stratio.meta.common.connector.ConnectorClusterConfig;
import com.stratio.meta.common.connector.IConfiguration;
import com.stratio.meta.common.connector.IConnector;
import com.stratio.meta.common.exceptions.ConnectionException;
import com.stratio.meta.common.exceptions.InitializationException;
import com.stratio.meta.common.security.ICredentials;
import com.stratio.meta2.common.data.ClusterName;
import com.stratio.meta2.common.metadata.ColumnType;

/**
 * Created by jmgomez on 4/09/14.
 */
public class StreamingConnectorHelper implements IConnectorHelper {

    
	private static final String ZOOKEEPER_PORT = "2181";
	private static final String ZOOKEEPER_SERVER = "10.200.0.58";//"192.168.0.2";
	protected String KAFKA_SERVER = "10.200.0.58";//"192.168.0.2";
    private String KAFKA_PORT = "9092";
	private ClusterName clusterName;



    public StreamingConnectorHelper(ClusterName clusterName) throws ConnectionException, InitializationException {
        super();
        this.clusterName = clusterName;
    }
    @Override
    public IConnector getConnector() {
		return new StreamingConnector();
        
    }

    @Override
    public IConfiguration getConfiguration() {
        return mock(IConfiguration.class);
    }

    @Override
    public ConnectorClusterConfig getConnectorClusterConfig() {
        Map<String, String> optionsNode = new HashMap<>();
     
        optionsNode.put("KafkaServer", KAFKA_SERVER);
        optionsNode.put("KafkaPort", KAFKA_PORT);
        optionsNode.put("zooKeeperServer", ZOOKEEPER_SERVER);
        optionsNode.put("zooKeeperPort", ZOOKEEPER_PORT);

        return new ConnectorClusterConfig(clusterName, optionsNode);
    }

    @Override
    public ICredentials getICredentials() {
        return mock(ICredentials.class);
    }

    @Override
    public Map<String, Object> recoveredCatalogSettings(String indexName) {

        Map<String, Object> result = new HashMap<>();

        fail("Not yet Streaming supported");
        return result;
    }

    @Override
    public Collection<ColumnType> getAllSupportedColumnType() {
        Set<ColumnType> allColumntTypes = new HashSet<>();

        allColumntTypes.add(ColumnType.BIGINT);
        allColumntTypes.add(ColumnType.BOOLEAN);
        allColumntTypes.add(ColumnType.DOUBLE);
        allColumntTypes.add(ColumnType.FLOAT);
        allColumntTypes.add(ColumnType.INT);
        allColumntTypes.add(ColumnType.TEXT);
        allColumntTypes.add(ColumnType.VARCHAR);
        return allColumntTypes;
    }

    @Override
    public boolean containsIndex(String catalogName, String collectionName, String indexName) {
        fail("Not yet Streaming supported");
        return false;
    }

    @Override
    public int countIndexes(String catalogName, String collectionName) {
        fail("Not yet Streaming supported");
        return 0;
    }



    @Override
    public void refresh(String schema) {
    
       

    }
	@Override
	public boolean isCatalogMandatory() {
	
		return false;
	}

    @Override public boolean isTableMandatory() {
        return true;
    }

}
