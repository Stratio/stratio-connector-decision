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

import java.util.ArrayList;
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

public class ThreadTwoFilterFunctionalTest extends GenericStreamingTest {

    public static final int CORRECT_ELMENT_TO_FIND = 90;
    private static final int WAIT_TIME = 20;
    private static final String TEXT_FIND = "text find";
    public int OTHER_INT_VALUE = 5;
    public int recoveredRecord = 0;
    TableMetadata tableMetadata;
    int numberDefaultText = 0;
    int numberAlternativeText = 0;
    private int correctValueCount = 0;
    private int incorrectValueCount = 0;
    private ArrayList<String> recovered = new ArrayList<>();

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
    public void testTwoFilter() throws InterruptedException, UnsupportedException {

        StreamingRead stremingRead = new StreamingRead(sConnector, getClusterName(), tableMetadata,
                createTwoFilterWorkFlow(), new ResultNumberHandler());

        stremingRead.start();
        System.out.println("TEST ********************** Querying......");
        waitSeconds(WAIT_TIME);

        System.out.println("TEST ********************** Inserting ......");
        StreamingInserter stramingInserter = new StreamingInserter(sConnector, getClusterName(), tableMetadata);
        stramingInserter.changeIntegerChangeableColumn(OTHER_INT_VALUE);
        stramingInserter.start();

        StreamingInserter otherStreamingInserter = new StreamingInserter(sConnector, getClusterName(), tableMetadata);
        otherStreamingInserter.changeStingColumn(TEXT_FIND);

        otherStreamingInserter.start();

        // This is the correct inserter.
        StreamingInserter correctStreamingInserter = new StreamingInserter(sConnector, getClusterName(), tableMetadata);
        correctStreamingInserter.changeStingColumn(TEXT_FIND);
        correctStreamingInserter.changeIntegerChangeableColumn(OTHER_INT_VALUE - 1);
        correctStreamingInserter.numOfElement(CORRECT_ELMENT_TO_FIND); // Desiere element number
        correctStreamingInserter.start();

        waitSeconds(WAIT_TIME);

        stremingRead.end();

        waitSeconds(WAIT_TIME);
        System.out.println("TEST ********************** END Querying Test......");

        otherStreamingInserter.end();

        stramingInserter.end();

        System.out.println("TEST ********************** END Insert......");

        assertEquals("All correct elements have been found", CORRECT_ELMENT_TO_FIND, recoveredRecord);

        for (String recover : recovered) {
            System.out.println(recover);
        }

    }

    private LogicalWorkflow createTwoFilterWorkFlow() throws UnsupportedException {
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
                .addEqualFilter(STRING_COLUMN, TEXT_FIND, false, false).addWindow(WindowType.TEMPORAL, 5)
                .getLogicalWorkflow();
    }

    private class ResultNumberHandler implements IResultHandler {

        public ResultNumberHandler() {

        }

        @Override
        public void processException(String queryId, ExecutionException exception) {
            System.out.println(queryId + " " + exception.getMessage());
            exception.printStackTrace();
        }

        @Override
        public void processResult(QueryResult result) {

            for (Row row : result.getResultSet()) {
                recovered.add(INTEGER_CHANGEABLE_COLUMN + "="
                        + ((Double) row.getCell(INTEGER_CHANGEABLE_COLUMN).getValue()).intValue() + ","
                        + STRING_COLUMN + "=" + row.getCell(STRING_COLUMN).getValue());

                recoveredRecord++;

            }

        }

    }

}
