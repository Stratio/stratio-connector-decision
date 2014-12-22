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
package com.stratio.connector.streaming.core.engine;

import static org.mockito.Matchers.anyList;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.powermock.modules.junit4.PowerMockRunner;

import com.stratio.connector.commons.connection.Connection;
import com.stratio.connector.commons.connection.ConnectionHandler;
import com.stratio.crossdata.common.data.AlterOperation;
import com.stratio.crossdata.common.data.AlterOptions;
import com.stratio.crossdata.common.data.ClusterName;
import com.stratio.crossdata.common.data.ColumnName;
import com.stratio.crossdata.common.data.IndexName;
import com.stratio.crossdata.common.data.TableName;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.exceptions.UnsupportedException;
import com.stratio.crossdata.common.metadata.ColumnMetadata;
import com.stratio.crossdata.common.metadata.ColumnType;
import com.stratio.crossdata.common.metadata.IndexMetadata;
import com.stratio.crossdata.common.metadata.TableMetadata;
import com.stratio.crossdata.common.statements.structures.Selector;
import com.stratio.streaming.api.IStratioStreamingAPI;

/**
 * StreamingMetadataEngine Tester.
 *
 */
@RunWith(PowerMockRunner.class)
public class StreamingMetadataEngineTest {

    private static final String CATALOG = "catalog";
    private static final String TABLE = "table";
    private static final String CLUSTER_NAME = "cluster_name";
    private static final String COLUM = "column";
    StreamingMetadataEngine streamingMetadataEngine;
    @Mock
    ConnectionHandler connectionHandler;
    @Mock
    Connection<IStratioStreamingAPI> connection;
    @Mock
    com.stratio.streaming.api.IStratioStreamingAPI streamingApi;

    @Before
    public void before() throws Exception {

        when(connection.getNativeConnection()).thenReturn(streamingApi);
        streamingMetadataEngine = new StreamingMetadataEngine(connectionHandler);

    }

    /**
     * Method: createCatalog(CatalogMetadata indexMetaData, Connection<IStratioStreamingAPI> connection)
     */
    @Test(expected = UnsupportedException.class)
    public void createCatalogTest() throws Exception {
        streamingMetadataEngine.createCatalog(null, (Connection) null);
    }

    /**
     * Method: createTable(TableMetadata streamMetadata, Connection<IStratioStreamingAPI> connection)
     */
    @Test
    public void createTableTest() throws Exception {

        Map<Selector, Selector> options = Collections.EMPTY_MAP;
        Map<IndexName, IndexMetadata> index = Collections.EMPTY_MAP;
        Map<ColumnName, ColumnMetadata> columns = new LinkedHashMap<>();
        ColumnMetadata columnMetadata = new ColumnMetadata(new ColumnName(CATALOG, TABLE, COLUM), new Object[0],
                        ColumnType.INT);
        columns.put(new ColumnName(CATALOG, TABLE, COLUM), columnMetadata);

        List<ColumnName> partitionKey = Collections.EMPTY_LIST;
        List<ColumnName> clusterKey = Collections.EMPTY_LIST;
        TableMetadata tableMetadata = new TableMetadata(new TableName(CATALOG, TABLE), options, columns, index,
                        new ClusterName(CLUSTER_NAME), partitionKey, clusterKey);
        streamingMetadataEngine.createTable(tableMetadata, connection);

        verify(streamingApi, times(1)).createStream(eq(CATALOG + "_" + TABLE), anyList());
    }

    /**
     * Method: alterTable(TableMetadata streamMetadata, Connection<IStratioStreamingAPI> connection)
     */
    @Test
    public void alterTableAddColumnTest() throws Exception {

        ColumnMetadata columnMetadata = new ColumnMetadata(new ColumnName(CATALOG, TABLE, COLUM), new Object[0],
                        ColumnType.INT);
        TableName tableName = new TableName(CATALOG, TABLE);
        AlterOptions alterOptions = new AlterOptions(AlterOperation.ADD_COLUMN, null, columnMetadata);
        streamingMetadataEngine.alterTable(tableName, alterOptions, connection);

        verify(streamingApi, times(1)).alterStream(eq(CATALOG + "_" + TABLE), Matchers.anyList());
    }

    /**
     * Method: alterTableNotSupportedTest(TableMetadata streamMetadata, Connection<IStratioStreamingAPI> connection)
     */
    @Test(expected = ExecutionException.class)
    public void alterTableNotSupportedTest() throws Exception {
        TableName tableName = new TableName(CATALOG, TABLE);
        AlterOptions alterOptions = new AlterOptions(AlterOperation.DROP_COLUMN, null, null);
        streamingMetadataEngine.alterTable(tableName, alterOptions, connection);
    }

    /**
     * Method: dropCatalog(CatalogName indexName, Connection<IStratioStreamingAPI> connection)
     */
    @Test(expected = UnsupportedException.class)
    public void dropCatalogTest() throws Exception {
        streamingMetadataEngine.dropCatalog(null, (Connection) null);

    }

    /**
     * Method: dropTable(TableName stream, Connection<IStratioStreamingAPI> connection)
     */
    @Test
    public void dropTableTest() throws Exception {

        streamingMetadataEngine.dropTable(new TableName(CATALOG, TABLE), connection);

        verify(streamingApi, times(1)).dropStream(CATALOG + "_" + TABLE);
    }

    /**
     * Method: createIndex(IndexMetadata indexMetadata, Connection<IStratioStreamingAPI> connection)
     */
    @Test(expected = UnsupportedException.class)
    public void createIndexTest() throws Exception {
        streamingMetadataEngine.createIndex(null, (Connection) null);
    }

    /**
     * Method: dropIndex(IndexMetadata indexMetadata, Connection<IStratioStreamingAPI> connection)
     */
    @Test(expected = UnsupportedException.class)
    public void dropIndexTest() throws Exception {
        streamingMetadataEngine.dropIndex(null, (Connection) null);
    }

}
