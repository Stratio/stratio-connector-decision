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
import java.util.List;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stratio.connector.commons.metadata.TableMetadataBuilder;
import com.stratio.connector.commons.test.util.LogicalWorkFlowCreator;
import com.stratio.connector.streaming.ftest.GenericStreamingTest;
import com.stratio.connector.streaming.ftest.thread.actions.RowToInsertDefault;
import com.stratio.connector.streaming.ftest.thread.actions.StreamingInserter;
import com.stratio.connector.streaming.ftest.thread.actions.StreamingRead;
import com.stratio.crossdata.common.connector.IResultHandler;
import com.stratio.crossdata.common.data.Cell;
import com.stratio.crossdata.common.data.Row;
import com.stratio.crossdata.common.data.TableName;
import com.stratio.crossdata.common.exceptions.ConnectorException;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.exceptions.UnsupportedException;
import com.stratio.crossdata.common.logicalplan.LogicalWorkflow;
import com.stratio.crossdata.common.logicalplan.Select;
import com.stratio.crossdata.common.metadata.ColumnMetadata;
import com.stratio.crossdata.common.metadata.ColumnType;
import com.stratio.crossdata.common.metadata.DataType;
import com.stratio.crossdata.common.metadata.TableMetadata;
import com.stratio.crossdata.common.result.QueryResult;
import com.stratio.crossdata.common.statements.structures.ColumnSelector;
import com.stratio.crossdata.common.statements.structures.window.WindowType;

public class ThreadTimeWindowFunctionalFT extends GenericStreamingTest {

    public static final int ELEMENTS_WRITE = 500;
    private static final String OTHER_TEXT = "OTHER...... ";
    private static final int WAIT_TIME = 15;
    /**
     * The Log.
     */
    final Logger logger = LoggerFactory.getLogger(this.getClass());
    boolean correctOrder = true;
    Set<Integer> returnSet = new HashSet<>();

    TableMetadata tableMetadata;

    Boolean correct = true;
    Boolean correctType = true;
    Boolean returnTypes = true;

    @Before
    public void setUp() throws ConnectorException {
        super.setUp();

        returnSet = new HashSet<>();
        correct = true;
        correctType = true;
        returnTypes = true;
        correctOrder = true;

        TableMetadataBuilder tableMetadataBuilder = new TableMetadataBuilder(CATALOG, TABLE);
        tableMetadata = tableMetadataBuilder.addColumn(STRING_COLUMN, new ColumnType(DataType.VARCHAR))
                        .addColumn(INTEGER_COLUMN, new ColumnType(DataType.INT)).addColumn(BOOLEAN_COLUMN, new ColumnType(DataType.BOOLEAN))
                        .addColumn(INTEGER_CHANGEABLE_COLUMN, new ColumnType(DataType.INT))
                        .build(false);
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
    public void stopReadBeforeStopWriteTest() throws InterruptedException, UnsupportedException {

        logger.debug("********************** Inserting ......");
        StreamingInserter stramingInserter = new StreamingInserter(sConnector, getClusterName(), tableMetadata, new
                RowToInsertDefault());
        stramingInserter.setAddIntegerChangeable(true);
        stramingInserter.start();

        LogicalWorkflow logicalWokflow = createLogicalWorkFlow();

        StreamingRead stremingRead = new StreamingRead(sConnector, logicalWokflow, new ResultHandler(
                        (Select) logicalWokflow.getLastStep()));

        stremingRead.start();
        logger.debug("********************** Querying......");
        waitSeconds(WAIT_TIME);

        stremingRead.end();
        logger.debug("********************** END Querying Test......");
        waitSeconds(WAIT_TIME);
        logger.debug(" ********************** Change Test Querying......");
        stramingInserter.changeStingColumn(OTHER_TEXT);
        waitSeconds(WAIT_TIME);

        logger.debug(" ********************** END Insert......");
        stramingInserter.end();
        waitSeconds(WAIT_TIME);

        assertTrue("all is correct", correct);
        assertTrue("Result is ordered", correctOrder);
        assertTrue("The types are correct", correctType);
        assertTrue("Return types", returnTypes);

    }

    @Test
    public void stopWriteBeforeStopReadTest() throws InterruptedException, UnsupportedException {

        StreamingInserter stramingInserter = new StreamingInserter(sConnector, getClusterName(), tableMetadata, new
                RowToInsertDefault());
        stramingInserter.setAddIntegerChangeable(true);
        stramingInserter.start();

        LogicalWorkflow logicalWokflow = createLogicalWorkFlow();

        ResultHandler resultHandler = new ResultHandler((Select) logicalWokflow.getLastStep());
        StreamingRead stremingRead = new StreamingRead(sConnector, logicalWokflow, resultHandler);

        stremingRead.start();
        logger.debug("********************** Querying......");
        waitSeconds(WAIT_TIME);

        logger.debug("********************** END Insert......");
        stramingInserter.end();
        waitSeconds(30); // it must be at least bigger than the windows time
        resultHandler.mustNotReadMore();
        logger.debug("TEST ********************** Wait for stoped read......");
        waitSeconds(10 * WAIT_TIME);

        stremingRead.end();
        logger.debug("TEST ********************** END Querying......");

        assertTrue("all is correct", correct);
        assertTrue("Result is ordered", correctOrder);
        assertTrue("The types are correct", correctType);
        assertTrue("Return types", returnTypes);

    }

    @Test
    public void insertConcreteNumberTest() throws InterruptedException, UnsupportedException {

        LogicalWorkflow logicalWokflow = createLogicalWorkFlow();

        StreamingRead stremingRead = new StreamingRead(sConnector, logicalWokflow, new ResultHandler(
                        (Select) logicalWokflow.getLastStep()));

        stremingRead.start();
        logger.debug("********************** Querying......");
        waitSeconds(WAIT_TIME);

        StreamingInserter stramingInserter = new StreamingInserter(sConnector, getClusterName(), tableMetadata, new
                RowToInsertDefault());
        stramingInserter.numOfElement(ELEMENTS_WRITE).elementPerSecond(ELEMENTS_WRITE);
        stramingInserter.setAddIntegerChangeable(true);
        stramingInserter.start();
        waitSeconds(WAIT_TIME);
        stremingRead.end();
        stramingInserter.end();

        assertEquals("the numberDefaultText of elements read is correct", ELEMENTS_WRITE, returnSet.size());
        assertTrue("all is correct", correct);
        assertTrue("Result is ordered", correctOrder);
        assertTrue("The types are correct", correctType);
        assertTrue("Return types", returnTypes);

    }

    private LogicalWorkflow createLogicalWorkFlow() throws UnsupportedException {
        LogicalWorkFlowCreator logicalWorkFlowCreator = new LogicalWorkFlowCreator(CATALOG, TABLE, getClusterName());

        LinkedList<LogicalWorkFlowCreator.ConnectorField> selectColumns = new LinkedList<>();
        selectColumns.add(logicalWorkFlowCreator.createConnectorField(STRING_COLUMN, STRING_COLUMN, new ColumnType(DataType.TEXT)));
        selectColumns.add(logicalWorkFlowCreator.createConnectorField(INTEGER_COLUMN, INTEGER_COLUMN, new ColumnType(DataType.INT)));
        selectColumns.add(logicalWorkFlowCreator.createConnectorField(BOOLEAN_COLUMN, BOOLEAN_COLUMN,
        		new ColumnType(DataType.BOOLEAN)));
        return logicalWorkFlowCreator.addColumnName(STRING_COLUMN).addColumnName(INTEGER_COLUMN)
                        .addColumnName(BOOLEAN_COLUMN).addSelect(selectColumns).addWindow(WindowType.TEMPORAL, 5)
                        .build();
    }

    @Test
    public void manyThreadTest() throws ConnectorException, InterruptedException {
        LogicalWorkflow logicalWokflow = new LogicalWorkFlowCreator(CATALOG, TABLE, getClusterName())
                        .addColumnName(STRING_COLUMN).addColumnName(INTEGER_COLUMN).addColumnName(BOOLEAN_COLUMN)
                        .addWindow(WindowType.TEMPORAL, 20).build();
        sConnector.getQueryEngine().asyncExecute("query1", logicalWokflow,
                        new ResultHandler((Select) logicalWokflow.getLastStep()));
        Thread.sleep(WAIT_TIME);
        sConnector.getQueryEngine().asyncExecute("query2", logicalWokflow,
                        new ResultHandler((Select) logicalWokflow.getLastStep()));
        Thread.sleep(WAIT_TIME);
        sConnector.getQueryEngine().asyncExecute("query3", logicalWokflow,
                        new ResultHandler((Select) logicalWokflow.getLastStep()));
        Thread.sleep(WAIT_TIME);
        sConnector.getQueryEngine().stop("query3");
        sConnector.getQueryEngine().asyncExecute("query4", logicalWokflow,
                        new ResultHandler((Select) logicalWokflow.getLastStep()));
        Thread.sleep(WAIT_TIME);
        sConnector.getQueryEngine().stop("query2");
        sConnector.getQueryEngine().asyncExecute("query5", logicalWokflow,
                        new ResultHandler((Select) logicalWokflow.getLastStep()));
        Thread.sleep(WAIT_TIME);
        sConnector.getQueryEngine().stop("query1");
        sConnector.getQueryEngine().asyncExecute("query6", logicalWokflow,
                        new ResultHandler((Select) logicalWokflow.getLastStep()));
        sConnector.getQueryEngine().stop("query4");

        Thread.sleep(WAIT_TIME);

        sConnector.getQueryEngine().stop("query5");
        sConnector.getQueryEngine().stop("query6");

    }

    private class ResultHandler implements IResultHandler {

        boolean mustRead = true;

        private ColumnSelector[] orderendColumnaName;

        public ResultHandler(Select select) {
            mustRead = true;
            orderendColumnaName = select.getColumnMap().keySet().toArray(new ColumnSelector[0]);

        }

        @Override
        public void processException(String queryId, ExecutionException exception) {
            logger.error(queryId + " " + exception.getMessage());
            exception.printStackTrace();
        }

        public void mustNotReadMore() {
            mustRead = false;

        }

        @Override
        public void processResult(QueryResult result) {
            if (!mustRead && result.getResultSet().size() != 0) {
                correct = false;
                System.out.println(result.getResultSet().size());
            }

            testTypes(result);
            for (Row row : result.getResultSet()) {
                testOrder(row);
                testElementNumber(row);
            }

        }

        private void testElementNumber(Row row) {
            Integer cellValue = (Integer) row.getCell(INTEGER_COLUMN).getValue();
            returnSet.add(cellValue); // To remove duplicates
            Cell cell = row.getCell(STRING_COLUMN);
            if (cell != null) {
                Object value = cell.getValue();
                if (OTHER_TEXT.equals(value)) {
                    correct = false;
                }
            }
        }

        private void testOrder(Row row) {
            String[] recoveredColumn = row.getCells().keySet().toArray(new String[0]);
            for (int i = 0; i < recoveredColumn.length; i++) {
                if (!orderendColumnaName[i].getName().getName().equals(recoveredColumn[i])) {
                    correctOrder = false;
                }
            }
        }

        private void testTypes(QueryResult result) {
            List<ColumnMetadata> columnMetadataList = result.getResultSet().getColumnMetadata();
            if (columnMetadataList == null) {
                returnTypes = false;
            } else {
                ColumnMetadata[] columnMetadata = columnMetadataList.toArray(new ColumnMetadata[0]);

                if (!columnMetadata[0].getColumnType().equals(new ColumnType(DataType.TEXT))
                                || !columnMetadata[1].getColumnType().equals(new ColumnType(DataType.INT))
                                || !columnMetadata[2].getColumnType().equals(new ColumnType(DataType.BOOLEAN))) {
                    correctType = false;
                }
            }
        }

    }

}
