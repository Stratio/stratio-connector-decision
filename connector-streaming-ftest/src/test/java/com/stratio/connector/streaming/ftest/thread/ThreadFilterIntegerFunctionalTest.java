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

import com.stratio.connector.commons.ftest.schema.TableMetadataBuilder;
import com.stratio.connector.commons.ftest.workFlow.LogicalWorkFlowCreator;
import com.stratio.connector.streaming.ftest.GenericStreamingTest;
import com.stratio.connector.streaming.ftest.thread.actions.StreamingInserter;
import com.stratio.connector.streaming.ftest.thread.actions.StreamingRead;
import com.stratio.crossdata.common.connector.IResultHandler;
import com.stratio.crossdata.common.data.Row;
import com.stratio.crossdata.common.exceptions.ConnectorException;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.exceptions.UnsupportedException;
import com.stratio.crossdata.common.logicalplan.LogicalWorkflow;
import com.stratio.crossdata.common.metadata.ColumnType;
import com.stratio.crossdata.common.metadata.TableMetadata;
import com.stratio.crossdata.common.result.QueryResult;
import com.stratio.crossdata.common.statements.structures.window.WindowType;

public class ThreadFilterIntegerFunctionalTest extends GenericStreamingTest {

    public static final int CORRECT_ELMENT_TO_FIND = 90; // Must be a time window
    private static final int WAIT_TIME = 20;
    public int OTHER_INT_VALUE = 5;
    TableMetadata tableMetadata;
    int numberDefaultText = 0;
    int numberAlternativeText = 0;
    private int DEFAULT_INT_VALUE = 10;
    private int correctValueCount = 0;
    private int incorrectValueCount = 0;

    @Before
    public void setUp() throws ConnectorException {
        super.setUp();
        numberDefaultText = 0;
        numberAlternativeText = 0;
        correctValueCount = 0;
        incorrectValueCount = 0;

        TableMetadataBuilder tableMetadataBuilder = new TableMetadataBuilder(CATALOG, TABLE);
        tableMetadata = tableMetadataBuilder.addColumn(STRING_COLUMN, ColumnType.VARCHAR)
                        .addColumn(INTEGER_COLUMN, ColumnType.INT).addColumn(BOOLEAN_COLUMN, ColumnType.BOOLEAN)
                        .addColumn(INTEGER_CHANGEABLE_COLUMN, ColumnType.INT).build();
        try {
            sConnector.getMetadataEngine().createTable(getClusterName(), tableMetadata);

        } catch (ExecutionException t) {

        }

    }

    @Test
    public void testLowerFilter() throws InterruptedException, UnsupportedException {

        int correctValue = 4;
        int incorrectValue = OTHER_INT_VALUE;
        StreamingRead stremingRead = new StreamingRead(sConnector, getClusterName(), tableMetadata,
                        createLowerLogicalWorkFlow(), new ResultNumberHandler(correctValue, incorrectValue));

        stremingRead.start();
        System.out.println("TEST ********************** Querying......");
        waitSeconds(WAIT_TIME);

        System.out.println("TEST ********************** Inserting ......");
        StreamingInserter stramingInserter = new StreamingInserter(sConnector, getClusterName(), tableMetadata);
        stramingInserter.changeIntegerChangeableColumn(correctValue);
        stramingInserter.numOfElement(CORRECT_ELMENT_TO_FIND);
        stramingInserter.start();

        StreamingInserter oherStreamingInserter = new StreamingInserter(sConnector, getClusterName(), tableMetadata);
        oherStreamingInserter.changeIntegerChangeableColumn(OTHER_INT_VALUE);

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

        assertEquals("Don't exist incorrect elements", 0, incorrectValueCount);
        assertEquals("All correct elements have been found", CORRECT_ELMENT_TO_FIND, correctValueCount);

    }

    @Test
    public void testGreatFilter() throws InterruptedException, UnsupportedException {

        int correctValue = 6;
        int incorrectValue = OTHER_INT_VALUE;
        StreamingRead stremingRead = new StreamingRead(sConnector, getClusterName(), tableMetadata,
                        createGreatLogicalWorkFlow(), new ResultNumberHandler(correctValue, incorrectValue));

        stremingRead.start();
        System.out.println("TEST ********************** Querying......");
        waitSeconds(WAIT_TIME);

        System.out.println("TEST ********************** Inserting ......");
        StreamingInserter stramingInserter = new StreamingInserter(sConnector, getClusterName(), tableMetadata);
        stramingInserter.changeIntegerChangeableColumn(correctValue);
        stramingInserter.numOfElement(CORRECT_ELMENT_TO_FIND);
        stramingInserter.start();

        StreamingInserter oherStreamingInserter = new StreamingInserter(sConnector, getClusterName(), tableMetadata);
        oherStreamingInserter.changeIntegerChangeableColumn(incorrectValue);

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

        assertEquals("Don't exist incorrect elements", 0, incorrectValueCount);
        assertEquals("All correct elements have been found", CORRECT_ELMENT_TO_FIND, correctValueCount);

    }

    @Test
    public void testGreatEqualsFilter() throws InterruptedException, UnsupportedException {

        int correctValue = DEFAULT_INT_VALUE;
        int incorrectValue = DEFAULT_INT_VALUE - 1;
        StreamingRead stremingRead = new StreamingRead(sConnector, getClusterName(), tableMetadata,
                        createGreatEqualLogicalWorkFlow(), new ResultNumberHandler(correctValue, incorrectValue));

        stremingRead.start();
        System.out.println("TEST ********************** Querying......");
        waitSeconds(WAIT_TIME);

        System.out.println("TEST ********************** Inserting ......");
        StreamingInserter stramingInserter = new StreamingInserter(sConnector, getClusterName(), tableMetadata);
        stramingInserter.changeIntegerChangeableColumn(correctValue);
        stramingInserter.numOfElement(CORRECT_ELMENT_TO_FIND);
        stramingInserter.start();

        StreamingInserter oherStreamingInserter = new StreamingInserter(sConnector, getClusterName(), tableMetadata);
        oherStreamingInserter.changeIntegerChangeableColumn(OTHER_INT_VALUE);

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

        assertEquals("Don't exist incorrect elements", 0, incorrectValueCount);
        assertEquals("All correct elements have been found", CORRECT_ELMENT_TO_FIND, correctValueCount);

    }

    @Test
    public void testLowerEqualsFilter() throws InterruptedException, UnsupportedException {

        StreamingRead stremingRead = new StreamingRead(sConnector, getClusterName(), tableMetadata,
                        createLowerEqualsLogicalWorkFlow(), new ResultNumberHandler(OTHER_INT_VALUE, DEFAULT_INT_VALUE));

        stremingRead.start();
        System.out.println("TEST ********************** Querying......");
        waitSeconds(WAIT_TIME);

        System.out.println("TEST ********************** Inserting ......");
        StreamingInserter stramingInserter = new StreamingInserter(sConnector, getClusterName(), tableMetadata);
        stramingInserter.changeIntegerChangeableColumn(OTHER_INT_VALUE);
        stramingInserter.numOfElement(CORRECT_ELMENT_TO_FIND);
        stramingInserter.start();

        StreamingInserter otherStreamingInserter = new StreamingInserter(sConnector, getClusterName(), tableMetadata);

        otherStreamingInserter.start();

        waitSeconds(WAIT_TIME);

        stremingRead.end();
        System.out.println("TEST ********************** END Querying Test......");
        waitSeconds(WAIT_TIME);

        System.out.println("TEST ********************** Change Test Querying......");
        waitSeconds(WAIT_TIME);

        System.out.println("TEST ********************** END Insert......");
        otherStreamingInserter.end();
        stramingInserter.end();

        assertEquals("All correct elements have been found", CORRECT_ELMENT_TO_FIND, correctValueCount);
        assertEquals("Don't exist incorrect elements", 0, incorrectValueCount);

    }

    private LogicalWorkflow createLowerLogicalWorkFlow() throws UnsupportedException {
        LogicalWorkFlowCreator logicalWorkFlowCreator = new LogicalWorkFlowCreator(CATALOG, TABLE, getClusterName());

        LinkedList<LogicalWorkFlowCreator.ConnectorField> selectColumns = new LinkedList<>();
        selectColumns.add(logicalWorkFlowCreator.createConnectorField(STRING_COLUMN, STRING_COLUMN, ColumnType.TEXT));
        selectColumns.add(logicalWorkFlowCreator.createConnectorField(INTEGER_COLUMN, INTEGER_COLUMN, ColumnType.INT));
        selectColumns.add(logicalWorkFlowCreator.createConnectorField(BOOLEAN_COLUMN, BOOLEAN_COLUMN,
                        ColumnType.BOOLEAN));
        selectColumns.add(logicalWorkFlowCreator.createConnectorField(INTEGER_CHANGEABLE_COLUMN,
                        INTEGER_CHANGEABLE_COLUMN, ColumnType.INT));

        return logicalWorkFlowCreator.addColumnName(STRING_COLUMN).addColumnName(INTEGER_COLUMN)
                        .addColumnName(BOOLEAN_COLUMN).addColumnName(INTEGER_CHANGEABLE_COLUMN)
                        .addSelect(selectColumns).addNLowerFilter(INTEGER_CHANGEABLE_COLUMN, OTHER_INT_VALUE, false)
                        .addWindow(WindowType.TEMPORAL, 5).getLogicalWorkflow();
    }

    private LogicalWorkflow createGreatEqualLogicalWorkFlow() throws UnsupportedException {
        LogicalWorkFlowCreator logicalWorkFlowCreator = new LogicalWorkFlowCreator(CATALOG, TABLE, getClusterName());

        LinkedList<LogicalWorkFlowCreator.ConnectorField> selectColumns = new LinkedList<>();
        selectColumns.add(logicalWorkFlowCreator.createConnectorField(STRING_COLUMN, STRING_COLUMN, ColumnType.TEXT));
        selectColumns.add(logicalWorkFlowCreator.createConnectorField(INTEGER_COLUMN, INTEGER_COLUMN, ColumnType.INT));
        selectColumns.add(logicalWorkFlowCreator.createConnectorField(BOOLEAN_COLUMN, BOOLEAN_COLUMN,
                        ColumnType.BOOLEAN));
        selectColumns.add(logicalWorkFlowCreator.createConnectorField(INTEGER_CHANGEABLE_COLUMN,
                        INTEGER_CHANGEABLE_COLUMN, ColumnType.INT));

        return logicalWorkFlowCreator.addColumnName(STRING_COLUMN).addColumnName(INTEGER_COLUMN)
                        .addColumnName(BOOLEAN_COLUMN).addColumnName(INTEGER_CHANGEABLE_COLUMN)
                        .addSelect(selectColumns)
                        .addGreaterEqualFilter(INTEGER_CHANGEABLE_COLUMN, OTHER_INT_VALUE, false, false)
                        .addWindow(WindowType.TEMPORAL, 5).getLogicalWorkflow();
    }

    private LogicalWorkflow createGreatLogicalWorkFlow() throws UnsupportedException {
        LogicalWorkFlowCreator logicalWorkFlowCreator = new LogicalWorkFlowCreator(CATALOG, TABLE, getClusterName());

        LinkedList<LogicalWorkFlowCreator.ConnectorField> selectColumns = new LinkedList<>();
        selectColumns.add(logicalWorkFlowCreator.createConnectorField(STRING_COLUMN, STRING_COLUMN, ColumnType.TEXT));
        selectColumns.add(logicalWorkFlowCreator.createConnectorField(INTEGER_COLUMN, INTEGER_COLUMN, ColumnType.INT));
        selectColumns.add(logicalWorkFlowCreator.createConnectorField(BOOLEAN_COLUMN, BOOLEAN_COLUMN,
                        ColumnType.BOOLEAN));
        selectColumns.add(logicalWorkFlowCreator.createConnectorField(INTEGER_CHANGEABLE_COLUMN,
                        INTEGER_CHANGEABLE_COLUMN, ColumnType.INT));

        return logicalWorkFlowCreator.addColumnName(STRING_COLUMN).addColumnName(INTEGER_COLUMN)
                        .addColumnName(BOOLEAN_COLUMN).addColumnName(INTEGER_CHANGEABLE_COLUMN)
                        .addSelect(selectColumns).addGreaterFilter(INTEGER_CHANGEABLE_COLUMN, OTHER_INT_VALUE, false)
                        .addWindow(WindowType.TEMPORAL, 5).getLogicalWorkflow();
    }

    private LogicalWorkflow createLowerEqualsLogicalWorkFlow() throws UnsupportedException {
        LogicalWorkFlowCreator logicalWorkFlowCreator = new LogicalWorkFlowCreator(CATALOG, TABLE, getClusterName());

        LinkedList<LogicalWorkFlowCreator.ConnectorField> selectColumns = new LinkedList<>();
        selectColumns.add(logicalWorkFlowCreator.createConnectorField(STRING_COLUMN, STRING_COLUMN, ColumnType.TEXT));
        selectColumns.add(logicalWorkFlowCreator.createConnectorField(INTEGER_COLUMN, INTEGER_COLUMN, ColumnType.INT));
        selectColumns.add(logicalWorkFlowCreator.createConnectorField(BOOLEAN_COLUMN, BOOLEAN_COLUMN,
                        ColumnType.BOOLEAN));
        selectColumns.add(logicalWorkFlowCreator.createConnectorField(INTEGER_CHANGEABLE_COLUMN,
                        INTEGER_CHANGEABLE_COLUMN, ColumnType.INT));

        return logicalWorkFlowCreator.addColumnName(STRING_COLUMN).addColumnName(INTEGER_COLUMN)
                        .addColumnName(BOOLEAN_COLUMN).addColumnName(INTEGER_CHANGEABLE_COLUMN)
                        .addSelect(selectColumns)
                        .addLowerEqualFilter(INTEGER_CHANGEABLE_COLUMN, OTHER_INT_VALUE, false)
                        .addWindow(WindowType.TEMPORAL, 2).getLogicalWorkflow();
    }

    private class ResultNumberHandler implements IResultHandler {

        int correctValue;
        int incorrectValue;

        public ResultNumberHandler(int correctValue, int incorrectValue) {
            this.correctValue = correctValue;
            this.incorrectValue = incorrectValue;
        }

        @Override
        public void processException(String queryId, ExecutionException exception) {
            System.out.println(queryId + " " + exception.getMessage());
            exception.printStackTrace();
        }

        @Override
        public void processResult(QueryResult result) {

            for (Row row : result.getResultSet()) {
                int value = (int) row.getCell(INTEGER_CHANGEABLE_COLUMN).getValue();
                System.out.println("********************>>" + value);

                if (correctValue == value) {

                    correctValueCount++;

                } else if (incorrectValue == value) {
                    // If streaming read random init value
                    incorrectValueCount++;
                }

            }

        }

    }

}
