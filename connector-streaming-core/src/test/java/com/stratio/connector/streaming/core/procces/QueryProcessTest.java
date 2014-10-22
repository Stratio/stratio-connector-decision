/*
* Licensed to STRATIO (C) under one or more contributor license agreements.
* See the NOTICE file distributed with this work for additional information
* regarding copyright ownership. The STRATIO (C) licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License. You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied. See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package com.stratio.connector.streaming.core.procces;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.internal.util.reflection.Whitebox;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.stratio.connector.commons.connection.Connection;
import com.stratio.connector.streaming.core.engine.query.ConnectorQueryBuilder;
import com.stratio.connector.streaming.core.engine.query.ConnectorQueryData;
import com.stratio.connector.streaming.core.engine.query.ConnectorQueryParser;
import com.stratio.connector.streaming.core.engine.query.queryexecutor.ConnectorQueryExecutor;
import com.stratio.crossdata.common.connector.IResultHandler;
import com.stratio.crossdata.common.data.TableName;
import com.stratio.crossdata.common.logicalplan.Project;
import com.stratio.streaming.api.IStratioStreamingAPI;

/**
 * QueryProcess Tester.
 *
 * @author <Authors name>
 * @version 1.0
 * @since <pre>
 * oct 17, 2014
 * </pre>
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ ConnectorQueryExecutor.class, QueryProcess.class })
public class QueryProcessTest {

    private static final String CATALOG = "catalog";
    private static final String TABLE = "table";
    String QUERY = "query";
    @Mock
    Connection<IStratioStreamingAPI> CONNECTION;
    @Mock
    IResultHandler RESULT_HANDLER;
    @Mock
    String QUERY_ID = "QUERY_ID";
    @Mock
    Project PROJECT;
    QueryProcess queryProcess;
    @Mock
    IStratioStreamingAPI streamingApi;

    @Mock
    ConnectorQueryParser queryParser;
    @Mock
    ConnectorQueryBuilder queryBuilder;
    @Mock
    ConnectorQueryExecutor connectorQueryExecutor;

    @Before
    public void before() throws Exception {

        when(CONNECTION.getNativeConnection()).thenReturn(streamingApi);
        queryProcess = new QueryProcess(QUERY_ID, PROJECT, RESULT_HANDLER, CONNECTION);
        Whitebox.setInternalState(queryProcess, "queryParser", queryParser);
        Whitebox.setInternalState(queryProcess, "queryBuilder", queryBuilder);

    }

    /**
     * Method: run()
     */
    @Test
    public void testRun() throws Exception {

        ConnectorQueryData connectorQueryData = mock(ConnectorQueryData.class);
        when(queryParser.transformLogicalWorkFlow(PROJECT, QUERY_ID)).thenReturn(connectorQueryData);
        when(queryBuilder.createQuery(connectorQueryData)).thenReturn(QUERY);
        PowerMockito.whenNew(ConnectorQueryExecutor.class).withArguments(connectorQueryData, RESULT_HANDLER)
                        .thenReturn(connectorQueryExecutor);
        queryProcess.run();

        verify(connectorQueryExecutor, times(1)).executeQuery(QUERY, CONNECTION);
    }

    /**
     * Method: endQuery()
     */
    @Test
    public void testEndQuery() throws Exception {

        Whitebox.setInternalState(queryProcess, "queryExecutor", connectorQueryExecutor);
        TableName tableName = new TableName(CATALOG, TABLE);
        when(PROJECT.getTableName()).thenReturn(tableName);

        queryProcess.endQuery();

        verify(connectorQueryExecutor, times(1)).endQuery(CATALOG + "_" + TABLE, CONNECTION);
    }

}
