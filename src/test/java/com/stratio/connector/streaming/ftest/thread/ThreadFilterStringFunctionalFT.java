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

import java.util.LinkedList;

import org.junit.Before;
import org.junit.Test;

import com.stratio.connector.commons.metadata.TableMetadataBuilder;
import com.stratio.connector.commons.test.util.LogicalWorkFlowCreator;
import com.stratio.connector.streaming.ftest.GenericStreamingTest;
import com.stratio.connector.streaming.ftest.thread.actions.RowToInsertDefault;
import com.stratio.connector.streaming.ftest.thread.actions.StreamingInserter;
import com.stratio.connector.streaming.ftest.thread.actions.StreamingRead;
import com.stratio.crossdata.common.connector.IResultHandler;
import com.stratio.crossdata.common.data.Row;
import com.stratio.crossdata.common.exceptions.ConnectorException;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.exceptions.UnsupportedException;
import com.stratio.crossdata.common.logicalplan.LogicalWorkflow;
import com.stratio.crossdata.common.metadata.ColumnType;
import com.stratio.crossdata.common.metadata.DataType;
import com.stratio.crossdata.common.metadata.TableMetadata;
import com.stratio.crossdata.common.result.QueryResult;
import com.stratio.crossdata.common.statements.structures.window.WindowType;

public class ThreadFilterStringFunctionalFT extends GenericStreamingTest {

    public static final int CORRECT_ELMENT_TO_FIND = 90; // Must be a time window
    public static final int OTHER_INT_VALUE = 5; // Must be a time window
    private static final String TEXT = "Text";
    private static final int DEFAULT_INT_VALUE = 10;
    private static final String OTHER_TEXT = "OTHER...... ";
    private static final int WAIT_TIME = 20;

    TableMetadata tableMetadata;

    int numberDefaultText = 0;
    int numberAlternativeText = 0;

    private int numberDefaultInt = 0;
    private int numberAlternativeInt = 0;

    @Before
    public void setUp() throws ConnectorException {
        super.setUp();
        numberDefaultText = 0;
        numberAlternativeText = 0;
        numberDefaultInt = 0;
        numberAlternativeInt = 0;

        TableMetadataBuilder tableMetadataBuilder = new TableMetadataBuilder(CATALOG, TABLE, CLUSTER_NAME);
        tableMetadata = tableMetadataBuilder.addColumn(STRING_COLUMN, new ColumnType(DataType.VARCHAR))
                        .addColumn(INTEGER_COLUMN, new ColumnType(DataType.INT)).addColumn(BOOLEAN_COLUMN, new ColumnType(DataType.BOOLEAN))
                        .addColumn(INTEGER_CHANGEABLE_COLUMN, new ColumnType(DataType.INT))
                        .build(false);
        try {
            sConnector.getMetadataEngine().createTable(getClusterName(), tableMetadata);

        } catch (ExecutionException t) {

        }

    }

    @Test
    public void testEqualFilter() throws InterruptedException, UnsupportedException {

        StreamingRead stremingRead = new StreamingRead(sConnector, createEqualLogicalWorkFlow(),
                        new ResultTextHandler());

        stremingRead.start();
        System.out.println("TEST ********************** Querying......");
        waitSeconds(WAIT_TIME);

        System.out.println("TEST ********************** Inserting ......");
        StreamingInserter streamingInserter = new StreamingInserter(sConnector, getClusterName(), tableMetadata, new RowToInsertDefault());
        streamingInserter.setAddIntegerChangeable(true);
        streamingInserter.numOfElement(CORRECT_ELMENT_TO_FIND);
        streamingInserter.start();

        StreamingInserter otherStreamingInserter = new StreamingInserter(sConnector, getClusterName(), tableMetadata, new RowToInsertDefault());
        otherStreamingInserter.changeStingColumn(OTHER_TEXT);
        otherStreamingInserter.start();

        waitSeconds(WAIT_TIME);

        stremingRead.end();
        System.out.println("TEST ********************** END Querying Test......");
        waitSeconds(WAIT_TIME);

        System.out.println("TEST ********************** Change Test Querying......");
        waitSeconds(WAIT_TIME);

        System.out.println("TEST ********************** END Insert......");
        otherStreamingInserter.end();
        streamingInserter.end();

        assertEquals("Do not exist incorrect elements", 0, numberAlternativeText);
        assertEquals("All correct elements have been found", CORRECT_ELMENT_TO_FIND, numberDefaultText);

    }

    @Test
    public void distinctFilterTest() throws InterruptedException, UnsupportedException {

        StreamingRead stremingRead = new StreamingRead(sConnector, createDistinctLogicalWorkFlow(),
                        new ResultTextHandler());

        stremingRead.start();
        System.out.println("TEST ********************** Querying......");
        waitSeconds(WAIT_TIME);

        System.out.println("TEST ********************** Inserting ......");
        StreamingInserter stramingInserter = new StreamingInserter(sConnector, getClusterName(), tableMetadata, new RowToInsertDefault());
        stramingInserter.setAddIntegerChangeable(true);
        stramingInserter.start();

        StreamingInserter oherStreamingInserter = new StreamingInserter(sConnector, getClusterName(), tableMetadata, new RowToInsertDefault());
        oherStreamingInserter.changeStingColumn(OTHER_TEXT);
        oherStreamingInserter.numOfElement(CORRECT_ELMENT_TO_FIND);
        oherStreamingInserter.start();

        waitSeconds(WAIT_TIME);

        stremingRead.end();
        System.out.println("TEST ********************** END Querying Test......");
        waitSeconds(WAIT_TIME);

        System.out.println("TEST ********************** Change Test Querying......");
        waitSeconds(WAIT_TIME);

        System.out.println("TEST ********************** END Insert......");
        oherStreamingInserter.end();
        stramingInserter.end();
        waitSeconds(WAIT_TIME);

        assertEquals("Don't exist incorrect elements", 0, numberDefaultText);
        assertEquals("All correct elements have been found", CORRECT_ELMENT_TO_FIND, numberAlternativeText);

    }

    private LogicalWorkflow createEqualLogicalWorkFlow() throws UnsupportedException {
        LogicalWorkFlowCreator logicalWorkFlowCreator = new LogicalWorkFlowCreator(CATALOG, TABLE, getClusterName());

        LinkedList<LogicalWorkFlowCreator.ConnectorField> selectColumns = new LinkedList<>();
        selectColumns.add(logicalWorkFlowCreator.createConnectorField(STRING_COLUMN, STRING_COLUMN, new ColumnType(DataType.TEXT)));
        selectColumns.add(logicalWorkFlowCreator.createConnectorField(INTEGER_COLUMN, INTEGER_COLUMN, new ColumnType(DataType.INT)));
        selectColumns.add(logicalWorkFlowCreator.createConnectorField(BOOLEAN_COLUMN, BOOLEAN_COLUMN,
        		new ColumnType(DataType.BOOLEAN)));

        return logicalWorkFlowCreator.addColumnName(STRING_COLUMN).addColumnName(INTEGER_COLUMN)
                        .addColumnName(BOOLEAN_COLUMN).addSelect(selectColumns)
                        .addEqualFilter(STRING_COLUMN, TEXT, false, false).addWindow(WindowType.TEMPORAL, 10)
                        .build();
    }

    private LogicalWorkflow createDistinctLogicalWorkFlow() throws UnsupportedException {
        LogicalWorkFlowCreator logicalWorkFlowCreator = new LogicalWorkFlowCreator(CATALOG, TABLE, getClusterName());

        LinkedList<LogicalWorkFlowCreator.ConnectorField> selectColumns = new LinkedList<>();
        selectColumns.add(logicalWorkFlowCreator.createConnectorField(STRING_COLUMN, STRING_COLUMN, new ColumnType(DataType.TEXT)));
        selectColumns.add(logicalWorkFlowCreator.createConnectorField(INTEGER_COLUMN, INTEGER_COLUMN, new ColumnType(DataType.INT)));
        selectColumns.add(logicalWorkFlowCreator.createConnectorField(BOOLEAN_COLUMN, BOOLEAN_COLUMN,
        		new ColumnType(DataType.BOOLEAN)));

        return logicalWorkFlowCreator.addColumnName(STRING_COLUMN).addColumnName(INTEGER_COLUMN)
                        .addColumnName(BOOLEAN_COLUMN).addSelect(selectColumns)
                        .addDistinctFilter(STRING_COLUMN, TEXT, false, false).addWindow(WindowType.TEMPORAL, 5)
                        .build();

    }

    private class ResultTextHandler implements IResultHandler {

        @Override
        public void processException(String queryId, ExecutionException exception) {
            exception.printStackTrace();
        }

        @Override
        public void processResult(QueryResult result) {

            for (Row row : result.getResultSet()) {
                Object value = row.getCell(STRING_COLUMN).getValue();
                System.out.println("********************>>" + value);
                if (TEXT.equals(value)) {
                    numberDefaultText++;

                } else if (OTHER_TEXT.equals(value)) {
                    // If streaming read random init value
                    numberAlternativeText++;
                }

            }

        }

    }

}
