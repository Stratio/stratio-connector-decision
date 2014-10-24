/*
 * Licensed to STRATIO (C) under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information
 * regarding copyright ownership. The STRATIO (C) licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.stratio.connector.streaming.ftest.thread;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stratio.connector.commons.ftest.schema.TableMetadataBuilder;
import com.stratio.connector.commons.ftest.workFlow.LogicalWorkFlowCreator;
import com.stratio.connector.streaming.ftest.GenericStreamingTest;
import com.stratio.connector.streaming.ftest.thread.actions.StreamingInserter;
import com.stratio.connector.streaming.ftest.thread.actions.StreamingRead;
import com.stratio.crossdata.common.connector.IResultHandler;
import com.stratio.crossdata.common.data.TableName;
import com.stratio.crossdata.common.exceptions.ConnectorException;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.exceptions.UnsupportedException;
import com.stratio.crossdata.common.logicalplan.LogicalWorkflow;
import com.stratio.crossdata.common.metadata.ColumnType;
import com.stratio.crossdata.common.metadata.TableMetadata;
import com.stratio.crossdata.common.result.QueryResult;
import com.stratio.crossdata.common.statements.structures.window.WindowType;

public class ThreadNumberElementWindowFunctionalTest extends GenericStreamingTest {

    public static final int ELEMENTS_WRITE = 500;
    private static final int WINDOW_ELEMENTS = 10;
    public static String STRING_COLUMN = "string_column";
    public static String INTEGER_COLUMN = "integer_column";
    public static String BOOLEAN_COLUMN = "boolean_column";
    /**
     * The Log.
     */
    final Logger logger = LoggerFactory.getLogger(this.getClass());
    Set<Integer> returnSet = new HashSet<>();

    TableMetadata tableMetadata;

    Integer windowNumber = 0;
    Boolean correctNumberOfElement = true;

    @Before
    public void setUp() throws ConnectorException {
        super.setUp();

        returnSet = new HashSet<>();

        TableMetadataBuilder tableMetadataBuilder = new TableMetadataBuilder(CATALOG, TABLE);
        tableMetadata = tableMetadataBuilder.addColumn(STRING_COLUMN, ColumnType.VARCHAR)
                .addColumn(INTEGER_COLUMN, ColumnType.INT).addColumn(BOOLEAN_COLUMN, ColumnType.BOOLEAN)
                .addColumn(INTEGER_CHANGEABLE_COLUMN, ColumnType.INT).build();
        try {
            sConnector.getMetadataEngine().createTable(getClusterName(), tableMetadata);

        } catch (ExecutionException t) {

        }

    }

    @After
    public void tearDown() throws ConnectorException {
        sConnector.getMetadataEngine().dropTable(getClusterName(), new TableName(CATALOG, TABLE));
        sConnector.close(getClusterName());
    }

    @Test
    public void tesElmentWindow() throws InterruptedException, UnsupportedException {

        LogicalWorkFlowCreator logicalWorkFlowCreator = new LogicalWorkFlowCreator(CATALOG, TABLE, getClusterName());

        LinkedList<LogicalWorkFlowCreator.ConnectorField> selectColumns = new LinkedList<>();
        selectColumns.add(logicalWorkFlowCreator.createConnectorField(STRING_COLUMN, STRING_COLUMN, ColumnType.TEXT));
        selectColumns.add(logicalWorkFlowCreator.createConnectorField(INTEGER_COLUMN, INTEGER_COLUMN, ColumnType.INT));
        selectColumns.add(logicalWorkFlowCreator.createConnectorField(BOOLEAN_COLUMN, BOOLEAN_COLUMN,
                ColumnType.BOOLEAN));
        LogicalWorkflow logicalWokflow = logicalWorkFlowCreator.addColumnName(STRING_COLUMN)
                .addColumnName(INTEGER_COLUMN).addColumnName(BOOLEAN_COLUMN).addSelect(selectColumns)
                .addWindow(WindowType.NUM_ROWS, WINDOW_ELEMENTS).getLogicalWorkflow();

        StreamingRead stremingRead = new StreamingRead(sConnector, getClusterName(), tableMetadata, logicalWokflow,
                new ResultHandler());

        stremingRead.start();
        logger.debug(" ********************** Quering......");
        Thread.sleep(20000);

        StreamingInserter stramingInserter = new StreamingInserter(sConnector, getClusterName(), tableMetadata);
        stramingInserter.numOfElement(ELEMENTS_WRITE + 8).elementPerSecond(500);
        stramingInserter.start();
        Thread.sleep(30000);
        stremingRead.end();
        stramingInserter.end();
        Thread.sleep(10000);
        assertEquals("the numberDefaultText of windows is correct", ELEMENTS_WRITE / WINDOW_ELEMENTS,
                (Object) windowNumber);
        assertTrue("the elements in the windows are correct", correctNumberOfElement);

    }

    private class ResultHandler implements IResultHandler {

        @Override
        public void processException(String queryId, ExecutionException exception) {
            System.out.println(queryId + " " + exception.getMessage());
            exception.printStackTrace();
        }

        @Override
        public void processResult(QueryResult result) {
            windowNumber++;
            if (result.getResultSet().size() != WINDOW_ELEMENTS) {
                correctNumberOfElement = false;

            }

        }

    }

}
