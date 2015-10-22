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
package com.stratio.connector.decision.core.engine;

import static org.mockito.Matchers.anyList;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.LinkedHashMap;
import java.util.LinkedList;

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
import com.stratio.crossdata.common.metadata.DataType;
import com.stratio.crossdata.common.metadata.IndexMetadata;
import com.stratio.crossdata.common.metadata.TableMetadata;
import com.stratio.crossdata.common.statements.structures.Selector;
import com.stratio.decision.api.IStratioStreamingAPI;

/**
 * DecisionMetadataEngine Tester.
 *
 */
@RunWith(PowerMockRunner.class)
public class DecisionMetadataEngineTest {

    private static final String CATALOG = "catalog";
    private static final String TABLE = "table";
    private static final String CLUSTER_NAME = "cluster_name";
    private static final String COLUM = "column";
    DecisionMetadataEngine decisionMetadataEngine;
    @Mock
    ConnectionHandler connectionHandler;
    @Mock
    Connection<IStratioStreamingAPI> connection;
    @Mock
    com.stratio.decision.api.IStratioStreamingAPI decisionApi;

    @Before
    public void before() throws Exception {

        when(connection.getNativeConnection()).thenReturn(decisionApi);
        decisionMetadataEngine = new DecisionMetadataEngine(connectionHandler);

    }

    /**
     * Method: createCatalog(CatalogMetadata indexMetaData, Connection<IStratioStreamingAPI> connection)
     */
    @Test(expected = UnsupportedException.class)
    public void createCatalogTest() throws Exception {
        decisionMetadataEngine.createCatalog(null, (Connection) null);
    }

    /**
     * Method: createTable(TableMetadata streamMetadata, Connection<IStratioStreamingAPI> connection)
     */
    @Test
    public void createTableTest() throws Exception {

        LinkedHashMap<Selector, Selector> options = new LinkedHashMap();
        LinkedHashMap<IndexName, IndexMetadata> index = new LinkedHashMap();
        LinkedHashMap<ColumnName, ColumnMetadata> columns = new LinkedHashMap<>();
        ColumnMetadata columnMetadata = new ColumnMetadata(new ColumnName(CATALOG, TABLE, COLUM), new Object[0],
        		new ColumnType(DataType.INT));
        columns.put(new ColumnName(CATALOG, TABLE, COLUM), columnMetadata);

        LinkedList<ColumnName> partitionKey = new LinkedList();
        LinkedList<ColumnName> clusterKey = new  LinkedList();
        TableMetadata tableMetadata = new TableMetadata(new TableName(CATALOG, TABLE), options, columns, index,
                        new ClusterName(CLUSTER_NAME), partitionKey, clusterKey);
        decisionMetadataEngine.createTable(tableMetadata, connection);

        verify(decisionApi, times(1)).createStream(eq(CATALOG + "_" + TABLE), anyList());
    }

    /**
     * Method: alterTable(TableMetadata streamMetadata, Connection<IStratioStreamingAPI> connection)
     */
    @Test
    public void alterTableAddColumnTest() throws Exception {

        ColumnMetadata columnMetadata = new ColumnMetadata(new ColumnName(CATALOG, TABLE, COLUM), new Object[0],
        		new ColumnType(DataType.INT));
        TableName tableName = new TableName(CATALOG, TABLE);
        AlterOptions alterOptions = new AlterOptions(AlterOperation.ADD_COLUMN, null, columnMetadata);
        decisionMetadataEngine.alterTable(tableName, alterOptions, connection);

        verify(decisionApi, times(1)).alterStream(eq(CATALOG + "_" + TABLE), Matchers.anyList());
    }

    /**
     * Method: alterTableNotSupportedTest(TableMetadata streamMetadata, Connection<IStratioStreamingAPI> connection)
     */
    @Test(expected = ExecutionException.class)
    public void alterTableNotSupportedTest() throws Exception {
        TableName tableName = new TableName(CATALOG, TABLE);
        AlterOptions alterOptions = new AlterOptions(AlterOperation.DROP_COLUMN, null, null);
        decisionMetadataEngine.alterTable(tableName, alterOptions, connection);
    }

    /**
     * Method: dropCatalog(CatalogName indexName, Connection<IStratioStreamingAPI> connection)
     */
    @Test(expected = UnsupportedException.class)
    public void dropCatalogTest() throws Exception {
        decisionMetadataEngine.dropCatalog(null, (Connection) null);

    }

    /**
     * Method: dropTable(TableName stream, Connection<IStratioStreamingAPI> connection)
     */
    @Test
    public void dropTableTest() throws Exception {

        decisionMetadataEngine.dropTable(new TableName(CATALOG, TABLE), connection);

        verify(decisionApi, times(1)).dropStream(CATALOG + "_" + TABLE);
    }

    /**
     * Method: createIndex(IndexMetadata indexMetadata, Connection<IStratioStreamingAPI> connection)
     */
    @Test(expected = UnsupportedException.class)
    public void createIndexTest() throws Exception {
        decisionMetadataEngine.createIndex(null, (Connection) null);
    }

    /**
     * Method: dropIndex(IndexMetadata indexMetadata, Connection<IStratioStreamingAPI> connection)
     */
    @Test(expected = UnsupportedException.class)
    public void dropIndexTest() throws Exception {
        decisionMetadataEngine.dropIndex(null, (Connection) null);
    }

}
