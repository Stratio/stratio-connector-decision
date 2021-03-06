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

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.powermock.modules.junit4.PowerMockRunner;

import com.stratio.connector.commons.connection.Connection;
import com.stratio.connector.commons.connection.ConnectionHandler;
import com.stratio.crossdata.common.data.Cell;
import com.stratio.crossdata.common.data.ClusterName;
import com.stratio.crossdata.common.data.ColumnName;
import com.stratio.crossdata.common.data.IndexName;
import com.stratio.crossdata.common.data.Row;
import com.stratio.crossdata.common.data.TableName;
import com.stratio.crossdata.common.exceptions.UnsupportedException;
import com.stratio.crossdata.common.metadata.ColumnMetadata;
import com.stratio.crossdata.common.metadata.ColumnType;
import com.stratio.crossdata.common.metadata.DataType;
import com.stratio.crossdata.common.metadata.IndexMetadata;
import com.stratio.crossdata.common.metadata.TableMetadata;
import com.stratio.crossdata.common.statements.structures.Selector;
import com.stratio.decision.api.IStratioStreamingAPI;

/**
 * DecisionStorageEngine Tester.
 *
 */
@RunWith(PowerMockRunner.class)
public class DecisionStorageEngineTest {

    private static final String CATALOG = "catalog";
    private static final String TABLE = "table";
    private static final String CLUSTER_NAME = "cluster_name";
    private static final String COLUM = "column";
    private static final Object VALUE1 = "value1";
    private static final Object VALUE2 = "value2";

    DecisionStorageEngine decisionStorageEngine;
    @Mock
    ConnectionHandler connectionHandler;
    @Mock
    Connection<IStratioStreamingAPI> connection;
    @Mock
    com.stratio.decision.api.IStratioStreamingAPI decisionApi;

    @Before
    public void before() throws Exception {
        when(connection.getNativeConnection()).thenReturn(decisionApi);
        decisionStorageEngine = new DecisionStorageEngine(connectionHandler);
    }

    /**
     * Method: insert(TableMetadata targetStream, Row row, Connection<IStratioStreamingAPI> connection)
     */
    @Test
    public void insertTest() throws Exception {

        Row row = createRow(VALUE1);

        decisionStorageEngine.insert(createTableMetadata(), row, false,connection);

        verify(decisionApi, times(1)).insertData(eq(CATALOG + "_" + TABLE), anyList());

    }

    /**
     * Method: insert(TableMetadata targetStream, Collection<Row> rows, Connection<IStratioStreamingAPI> connection)
     */
    @Test
    public void insertBulkTest() throws Exception {

        Collection<Row> rows = new LinkedList<>();
        rows.add(createRow(VALUE1));
        rows.add(createRow(VALUE2));

        decisionStorageEngine.insert(createTableMetadata(), rows, false,connection);

        verify(decisionApi, times(2)).insertData(eq(CATALOG + "_" + TABLE), anyList());

    }

    /**
     * Method: delete(TableName tableName, Collection<Filter> whereClauses, Connection<IStratioStreamingAPI> connection)
     */
    @Test(expected = UnsupportedException.class)
    public void deleteTest() throws Exception {
        decisionStorageEngine.delete(Matchers.any(TableName.class), null, connection);
    }

    /**
     * Method: update(TableName tableName, Collection<Relation> assignments, Collection<Filter> whereClauses,
     * Connection<IStratioStreamingAPI> connection)
     */
    @Test(expected = UnsupportedException.class)
    public void updateTest() throws Exception {
        decisionStorageEngine.update(Matchers.any(TableName.class), Matchers.anyCollection(),
                        Matchers.anyCollection(), connection);
    }

    /**
     * Method: truncate(TableName tableName,Connection<IStratioStreamingAPI> connection)
     */
    @Test(expected = UnsupportedException.class)
    public void truncateTest() throws Exception {
        decisionStorageEngine.truncate(Matchers.any(TableName.class), connection);
    }

    private Row createRow(Object value) {
        Row row = new Row();
        Map<String, Cell> cells = new LinkedHashMap<>();
        cells.put(COLUM, new Cell(value));
        row.setCells(cells);
        return row;
    }

    private TableMetadata createTableMetadata() {
        Map<Selector, Selector> options = Collections.EMPTY_MAP;
        LinkedHashMap<IndexName, IndexMetadata> index = new LinkedHashMap();
        LinkedHashMap<ColumnName, ColumnMetadata> columns = new LinkedHashMap<>();
        ColumnMetadata columnMetadata = new ColumnMetadata(new ColumnName(CATALOG, TABLE, COLUM), new Object[0],
                        new ColumnType(DataType.INT));
        columns.put(new ColumnName(CATALOG, TABLE, COLUM), columnMetadata);

        LinkedList<ColumnName> partitionKey = new LinkedList<>();
        LinkedList<ColumnName> clusterKey = new LinkedList<>();
        return new TableMetadata(new TableName(CATALOG, TABLE), options, columns, index, new ClusterName(CLUSTER_NAME),
                        partitionKey, clusterKey);


    }

}
