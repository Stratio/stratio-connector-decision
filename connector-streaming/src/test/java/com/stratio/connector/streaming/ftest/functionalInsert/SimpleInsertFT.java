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
package com.stratio.connector.streaming.ftest.functionalInsert;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import com.stratio.connector.commons.TimerJ;
import org.junit.Test;

import com.stratio.connector.commons.metadata.TableMetadataBuilder;
import com.stratio.connector.commons.test.util.LogicalWorkFlowCreator;
import com.stratio.connector.streaming.ftest.GenericStreamingTest;
import com.stratio.connector.streaming.ftest.ResultHandlerTest;
import com.stratio.connector.streaming.ftest.thread.actions.RowToInsertBigLong;
import com.stratio.connector.streaming.ftest.thread.actions.RowToInsertDefault;
import com.stratio.connector.streaming.ftest.thread.actions.StreamingInserter;
import com.stratio.connector.streaming.ftest.thread.actions.StreamingRead;
import com.stratio.crossdata.common.connector.IResultHandler;
import com.stratio.crossdata.common.data.ClusterName;
import com.stratio.crossdata.common.data.ResultSet;
import com.stratio.crossdata.common.data.Row;
import com.stratio.crossdata.common.exceptions.ConnectorException;
import com.stratio.crossdata.common.exceptions.UnsupportedException;
import com.stratio.crossdata.common.logicalplan.LogicalWorkflow;
import com.stratio.crossdata.common.metadata.ColumnMetadata;
import com.stratio.crossdata.common.metadata.ColumnType;
import com.stratio.crossdata.common.metadata.DataType;
import com.stratio.crossdata.common.metadata.TableMetadata;
import com.stratio.crossdata.common.statements.structures.window.WindowType;

public class SimpleInsertFT extends GenericStreamingTest {

    @Test
    public void insertIntTest() throws ConnectorException {
        ClusterName clusterName = getClusterName();

        TableMetadataBuilder tableMetadataBuilder = new TableMetadataBuilder(CATALOG, TABLE, clusterName.getName());
        TableMetadata tableMetadata = tableMetadataBuilder.addColumn(INTEGER_COLUMN, new ColumnType(DataType.INT))
                        .addColumn(STRING_COLUMN, new ColumnType(DataType.VARCHAR)).build(false);

        sConnector.getMetadataEngine().createTable(getClusterName(), tableMetadata);

        // TODO window with 1element
        LogicalWorkFlowCreator logicalWorkFlowCreator = new LogicalWorkFlowCreator(CATALOG, TABLE, clusterName);
        LinkedList<LogicalWorkFlowCreator.ConnectorField> selectColumns = new LinkedList<>();
        selectColumns.add(logicalWorkFlowCreator.createConnectorField(INTEGER_COLUMN, INTEGER_COLUMN, new ColumnType(DataType.INT)));
        selectColumns.add(logicalWorkFlowCreator.createConnectorField(STRING_COLUMN, STRING_COLUMN, new ColumnType(DataType.VARCHAR)));
        LogicalWorkflow logicalWorkFlow = logicalWorkFlowCreator.addColumnName(INTEGER_COLUMN)
                        .addColumnName(STRING_COLUMN).addWindow(WindowType.NUM_ROWS, 1).addSelect(selectColumns)
                        .build();

        ResultHandlerTest resultSet = new ResultHandlerTest();
        StreamingRead reader = new StreamingRead(sConnector, logicalWorkFlow, resultSet);

        String queryId = String.valueOf(Math.abs(random.nextLong()));
        reader.setQueryId(queryId);
        reader.start();
        waitSeconds(20);

        StreamingInserter streamingInserter = new StreamingInserter(sConnector, clusterName, tableMetadata,
                        new RowToInsertDefault());
        streamingInserter.numOfElement(10).elementPerSecond(5);
        streamingInserter.addTypeToInsert(new ColumnType(DataType.INT)).addTypeToInsert(new ColumnType(DataType.VARCHAR));
        streamingInserter.start();

        waitSeconds(15);

        streamingInserter.end();
        waitSeconds(10);
        reader.end();

        ResultSet resSet = resultSet.getResultSet(queryId);

        // TODO double
        List<Integer> numsRecovered = new ArrayList<Integer>(10);
        for (Row recoveredRow : resSet.getRows()) {
            if (recoveredRow.getCell(STRING_COLUMN).getValue().equals("Text")) {
                numsRecovered.add((Integer) recoveredRow.getCell(INTEGER_COLUMN).getValue());
            }
        }

        assertEquals(10, numsRecovered.size());

        List<ColumnMetadata> columnMetadata = resSet.getColumnMetadata();
        assertEquals(INTEGER_COLUMN, columnMetadata.get(0).getName().getName());

        for (int i = 0; i < 10; i++) {
            assertTrue(numsRecovered.contains(new Integer(i)));
        }

        assertTrue(columnMetadata.size() == 2);
        assertEquals(new ColumnType(DataType.INT), columnMetadata.get(0).getColumnType());
        assertTrue(resSet.getRows().get(0).getCell(INTEGER_COLUMN).getValue() instanceof Integer);

    }

    @Test
    public void insertLongTest() throws ConnectorException {

        TableMetadata tableMetadata = createTable();
        ResultHandlerTest resultSet = new ResultHandlerTest();
        String queryId = String.valueOf(Math.abs(random.nextLong()));

        StreamingRead reader = startReader(resultSet, queryId);

        insertValuesToStream(tableMetadata, new RowToInsertDefault());
        endReader(reader);

        ResultSet resultQueryIdSet = resultSet.getResultSet(queryId);

        List<ColumnMetadata> columnMetadata = resultQueryIdSet.getColumnMetadata();
        assertEquals(LONG_COLUMN, columnMetadata.get(0).getName().getName());

        // TODO double
        List<Long> numsRecovered = new ArrayList<Long>(10);
        for (Row recoveredRow : resultQueryIdSet.getRows()) {
            if (recoveredRow.getCell(STRING_COLUMN).getValue().equals("Text")) {
                Object longValue = recoveredRow.getCell(LONG_COLUMN).getValue();
                assertTrue("The type must be Long", longValue instanceof Long);
                numsRecovered.add((Long) longValue);
            }
        }

        assertEquals(10, numsRecovered.size());

        for (int i = 0; i < 10; i++) {
            assertTrue("The value  recovered is correct " + i, numsRecovered.contains(new Long(i)));
        }

        assertTrue(columnMetadata.size() == 2);
        assertEquals(new ColumnType(DataType.BIGINT), columnMetadata.get(0).getColumnType());
        assertTrue(resultQueryIdSet.getRows().get(0).getCell(LONG_COLUMN).getValue() instanceof Long);
    }

    @Test
    public void insertBigLongTest() throws ConnectorException {

        TableMetadata tableMetadata = createTable();
        ResultHandlerTest resultSet = new ResultHandlerTest();
        String queryId = String.valueOf(Math.abs(random.nextLong()));

        StreamingRead reader = startReader(resultSet, queryId);

        RowToInsertBigLong rowToInsert = new RowToInsertBigLong();

        insertValuesToStream(tableMetadata, rowToInsert);
        endReader(reader);

        ResultSet resultQueryIdSet = resultSet.getResultSet(queryId);

        List<ColumnMetadata> columnMetadata = resultQueryIdSet.getColumnMetadata();
        assertEquals(LONG_COLUMN, columnMetadata.get(0).getName().getName());

        // TODO double
        List<Long> numsRecovered = new ArrayList<Long>(10);
        for (Row recoveredRow : resultQueryIdSet.getRows()) {
            if (recoveredRow.getCell(STRING_COLUMN).getValue().equals("Text")) {
                Object longValue = recoveredRow.getCell(LONG_COLUMN).getValue();
                assertTrue("The type must be Long", longValue instanceof Long);
                numsRecovered.add((Long) longValue);
            }
        }

        assertEquals(10, numsRecovered.size());

        for (int i = 0; i < 10; i++) {

            assertTrue("The value is not the expected one " + rowToInsert.getBigLong(i),
                            numsRecovered.contains(rowToInsert.getBigLong(i)));
        }

        assertTrue(columnMetadata.size() == 2);
        assertEquals(new ColumnType(DataType.BIGINT), columnMetadata.get(0).getColumnType());
        assertTrue(resultQueryIdSet.getRows().get(0).getCell(LONG_COLUMN).getValue() instanceof Long);
    }

    private void endReader(StreamingRead reader) {
        waitSeconds(15);
        reader.end();
    }

    private void insertValuesToStream(TableMetadata tableMetadata, RowToInsertDefault rowToInsert)
                    throws ConnectorException {

        StreamingInserter streamingInserter = new StreamingInserter(sConnector, getClusterName(), tableMetadata,
                        rowToInsert);
        streamingInserter.numOfElement(10).elementPerSecond(5);
        streamingInserter.addTypeToInsert(new ColumnType(DataType.BIGINT)).addTypeToInsert(new ColumnType(DataType.VARCHAR));
        streamingInserter.start();

        waitSeconds(10);

        streamingInserter.end();
    }

    private StreamingRead startReader(IResultHandler resultHandlerTest, String queryId) throws UnsupportedException {
        // TODO window with 1element
        LogicalWorkFlowCreator logicalWorkFlowCreator = new LogicalWorkFlowCreator(CATALOG, TABLE, getClusterName());
        LinkedList<LogicalWorkFlowCreator.ConnectorField> selectColumns = new LinkedList<>();
        selectColumns.add(logicalWorkFlowCreator.createConnectorField(LONG_COLUMN, LONG_COLUMN, new ColumnType(DataType.BIGINT)));
        selectColumns.add(logicalWorkFlowCreator.createConnectorField(STRING_COLUMN, STRING_COLUMN, new ColumnType(DataType.VARCHAR)));
        LogicalWorkflow logicalWorkFlow = logicalWorkFlowCreator.addColumnName(LONG_COLUMN)
                        .addColumnName(STRING_COLUMN).addWindow(WindowType.NUM_ROWS, 1).addSelect(selectColumns)
                        .build();

        StreamingRead reader = new StreamingRead(sConnector, logicalWorkFlow, resultHandlerTest);

        reader.setQueryId(queryId);
        reader.start();
        waitSeconds(15);
        return reader;
    }

    private TableMetadata createTable() throws ConnectorException {
        ClusterName clusterName = getClusterName();
        TableMetadataBuilder tableMetadataBuilder = new TableMetadataBuilder(CATALOG, TABLE, clusterName.getName());
        TableMetadata tableMetadata = tableMetadataBuilder.addColumn(LONG_COLUMN, new ColumnType(DataType.BIGINT))
                        .addColumn(STRING_COLUMN, new ColumnType(DataType.VARCHAR)).build(false);

        sConnector.getMetadataEngine().createTable(clusterName, tableMetadata);
        return tableMetadata;
    }

    @Test
    public void insertBooleanTest() throws ConnectorException {
        ClusterName clusterName = getClusterName();

        TableMetadataBuilder tableMetadataBuilder = new TableMetadataBuilder(CATALOG, TABLE, clusterName.getName());
        TableMetadata tableMetadata = tableMetadataBuilder.addColumn(BOOLEAN_COLUMN, new ColumnType(DataType.BOOLEAN))
                        .addColumn(STRING_COLUMN, new ColumnType(DataType.VARCHAR)).build(false);

        sConnector.getMetadataEngine().createTable(getClusterName(), tableMetadata);

        // TODO window with 1element
        LogicalWorkFlowCreator logicalWorkFlowCreator = new LogicalWorkFlowCreator(CATALOG, TABLE, clusterName);
        LinkedList<LogicalWorkFlowCreator.ConnectorField> selectColumns = new LinkedList<>();
        selectColumns.add(logicalWorkFlowCreator.createConnectorField(BOOLEAN_COLUMN, BOOLEAN_COLUMN,
        		new ColumnType(DataType.BOOLEAN)));
        selectColumns.add(logicalWorkFlowCreator.createConnectorField(STRING_COLUMN, STRING_COLUMN, new ColumnType(DataType.VARCHAR)));
        LogicalWorkflow logicalWorkFlow = logicalWorkFlowCreator.addColumnName(BOOLEAN_COLUMN, STRING_COLUMN)
                        .addWindow(WindowType.NUM_ROWS, 1).addSelect(selectColumns).build();

        ResultHandlerTest resultSet = new ResultHandlerTest();
        StreamingRead reader = new StreamingRead(sConnector, logicalWorkFlow, resultSet);

        String queryId = String.valueOf(Math.abs(random.nextLong()));
        reader.setQueryId(queryId);
        reader.start();
        waitSeconds(15);

        StreamingInserter streamingInserter = new StreamingInserter(sConnector, clusterName, tableMetadata,
                        new RowToInsertDefault());
        streamingInserter.numOfElement(10).elementPerSecond(5);
        streamingInserter.start();
        streamingInserter.addTypeToInsert(new ColumnType(DataType.BOOLEAN)).addTypeToInsert(new ColumnType(DataType.VARCHAR));
        waitSeconds(10);

        streamingInserter.end();
        endReader(reader);

        ResultSet resSet = resultSet.getResultSet(queryId);

        List<ColumnMetadata> columnMetadata = resSet.getColumnMetadata();
        assertEquals(BOOLEAN_COLUMN, columnMetadata.get(0).getName().getName());

        // TODO double
        List<Boolean> numsRecovered = new ArrayList<Boolean>(10);
        for (Row recoveredRow : resSet.getRows()) {
            if (recoveredRow.getCell(STRING_COLUMN).getValue().equals("Text")) {
                numsRecovered.add((Boolean) recoveredRow.getCell(BOOLEAN_COLUMN).getValue());
                assertTrue((Boolean) recoveredRow.getCell(BOOLEAN_COLUMN).getValue());
            }
        }

        assertEquals(10, numsRecovered.size());

        assertTrue(columnMetadata.size() == 2);
        assertEquals(new ColumnType(DataType.BOOLEAN), columnMetadata.get(0).getColumnType());
        assertTrue(resSet.getRows().get(0).getCell(BOOLEAN_COLUMN).getValue() instanceof Boolean);
    }

    @Test
    public void insertFloatTest() throws ConnectorException {
        ClusterName clusterName = getClusterName();

        TableMetadataBuilder tableMetadataBuilder = new TableMetadataBuilder(CATALOG, TABLE, clusterName.getName());
        TableMetadata tableMetadata = tableMetadataBuilder.addColumn(FLOAT_COLUMN, new ColumnType(DataType.FLOAT))
                        .addColumn(STRING_COLUMN, new ColumnType(DataType.VARCHAR)).build(false);

        sConnector.getMetadataEngine().createTable(getClusterName(), tableMetadata);

        // TODO window with 1element
        LogicalWorkFlowCreator logicalWorkFlowCreator = new LogicalWorkFlowCreator(CATALOG, TABLE, clusterName);
        LinkedList<LogicalWorkFlowCreator.ConnectorField> selectColumns = new LinkedList<>();
        selectColumns.add(logicalWorkFlowCreator.createConnectorField(FLOAT_COLUMN, FLOAT_COLUMN, new ColumnType(DataType.FLOAT)));
        selectColumns.add(logicalWorkFlowCreator.createConnectorField(STRING_COLUMN, STRING_COLUMN, new ColumnType(DataType.VARCHAR)));
        LogicalWorkflow logicalWorkFlow = logicalWorkFlowCreator.addColumnName(FLOAT_COLUMN, STRING_COLUMN)
                        .addWindow(WindowType.NUM_ROWS, 1).addSelect(selectColumns).build();

        ResultHandlerTest resultSet = new ResultHandlerTest();
        StreamingRead reader = new StreamingRead(sConnector, logicalWorkFlow, resultSet);

        String queryId = String.valueOf(Math.abs(random.nextLong()));
        reader.setQueryId(queryId);
        reader.start();
        waitSeconds(15);

        StreamingInserter streamingInserter = new StreamingInserter(sConnector, clusterName, tableMetadata,
                        new RowToInsertDefault());
        streamingInserter.numOfElement(10).elementPerSecond(5);
        streamingInserter.start();
        streamingInserter.addTypeToInsert(new ColumnType(DataType.FLOAT)).addTypeToInsert(new ColumnType(DataType.VARCHAR));
        waitSeconds(10);

        streamingInserter.end();
        endReader(reader);

        ResultSet resSet = resultSet.getResultSet(queryId);

        List<ColumnMetadata> columnMetadata = resSet.getColumnMetadata();
        assertEquals(FLOAT_COLUMN, columnMetadata.get(0).getName().getName());

        // TODO double
        List<Float> numsRecovered = new ArrayList<Float>(10);
        for (Row recoveredRow : resSet.getRows()) {
            if (recoveredRow.getCell(STRING_COLUMN).getValue().equals("Text")) {
                numsRecovered.add((Float) recoveredRow.getCell(FLOAT_COLUMN).getValue());
            }
        }

        assertEquals(10, numsRecovered.size());

        for (int i = 0; i < 10; i++) {
            assertTrue(numsRecovered.contains(new Float(i + 0.5)));
        }

        assertTrue(columnMetadata.size() == 2);
        assertEquals(new ColumnType(DataType.FLOAT), columnMetadata.get(0).getColumnType());
        assertTrue(resSet.getRows().get(0).getCell(FLOAT_COLUMN).getValue() instanceof Float);

    }

    @Test
    public void insertDoubleTest() throws ConnectorException {
        ClusterName clusterName = getClusterName();

        TableMetadataBuilder tableMetadataBuilder = new TableMetadataBuilder(CATALOG, TABLE, clusterName.getName());
        TableMetadata tableMetadata = tableMetadataBuilder.addColumn(DOUBLE_COLUMN, new ColumnType(DataType.DOUBLE))
                        .addColumn(STRING_COLUMN, new ColumnType(DataType.VARCHAR)).build(false);

        sConnector.getMetadataEngine().createTable(getClusterName(), tableMetadata);

        // TODO window with 1element
        LogicalWorkFlowCreator logicalWorkFlowCreator = new LogicalWorkFlowCreator(CATALOG, TABLE, clusterName);
        LinkedList<LogicalWorkFlowCreator.ConnectorField> selectColumns = new LinkedList<>();
        selectColumns.add(logicalWorkFlowCreator.createConnectorField(DOUBLE_COLUMN, DOUBLE_COLUMN, new ColumnType(DataType.DOUBLE)));
        selectColumns.add(logicalWorkFlowCreator.createConnectorField(STRING_COLUMN, STRING_COLUMN, new ColumnType(DataType.VARCHAR)));
        LogicalWorkflow logicalWorkFlow = logicalWorkFlowCreator.addColumnName(DOUBLE_COLUMN, STRING_COLUMN)
                        .addWindow(WindowType.NUM_ROWS, 1).addSelect(selectColumns).build();

        ResultHandlerTest resultSet = new ResultHandlerTest();
        StreamingRead reader = new StreamingRead(sConnector, logicalWorkFlow, resultSet);

        String queryId = String.valueOf(Math.abs(random.nextLong()));
        reader.setQueryId(queryId);
        reader.start();
        waitSeconds(15);

        StreamingInserter streamingInserter = new StreamingInserter(sConnector, clusterName, tableMetadata,
                        new RowToInsertDefault());
        streamingInserter.numOfElement(10).elementPerSecond(5);
        streamingInserter.start();
        streamingInserter.addTypeToInsert(new ColumnType(DataType.DOUBLE)).addTypeToInsert(new ColumnType(DataType.VARCHAR));
        waitSeconds(10);

        streamingInserter.end();
        endReader(reader);

        ResultSet resSet = resultSet.getResultSet(queryId);

        List<ColumnMetadata> columnMetadata = resSet.getColumnMetadata();
        assertEquals(DOUBLE_COLUMN, columnMetadata.get(0).getName().getName());

        // TODO double
        List<Double> numsRecovered = new ArrayList<Double>(10);
        for (Row recoveredRow : resSet.getRows()) {
            if (recoveredRow.getCell(STRING_COLUMN).getValue().equals("Text")) {
                numsRecovered.add((Double) recoveredRow.getCell(DOUBLE_COLUMN).getValue());
            }
        }

        assertEquals(10, numsRecovered.size());

        for (int i = 0; i < 10; i++) {
            assertTrue(numsRecovered.contains(new Double(i + 0.5)));
        }

        assertTrue(columnMetadata.size() == 2);
        assertEquals(new ColumnType(DataType.DOUBLE), columnMetadata.get(0).getColumnType());
        assertTrue(resSet.getRows().get(0).getCell(DOUBLE_COLUMN).getValue() instanceof Double);

    }

    @Test
    public void insertStringTest() throws ConnectorException {
        ClusterName clusterName = getClusterName();

        TableMetadataBuilder tableMetadataBuilder = new TableMetadataBuilder(CATALOG, TABLE, clusterName.getName());
        TableMetadata tableMetadata = tableMetadataBuilder.addColumn(STRING_COLUMN, new ColumnType(DataType.VARCHAR)).build(false);

        sConnector.getMetadataEngine().createTable(getClusterName(), tableMetadata);

        // TODO window with 1element
        LogicalWorkFlowCreator logicalWorkFlowCreator = new LogicalWorkFlowCreator(CATALOG, TABLE, clusterName);
        LinkedList<LogicalWorkFlowCreator.ConnectorField> selectColumns = new LinkedList<>();
        selectColumns.add(logicalWorkFlowCreator.createConnectorField(STRING_COLUMN, STRING_COLUMN, new ColumnType(DataType.VARCHAR)));
        LogicalWorkflow logicalWorkFlow = logicalWorkFlowCreator.addColumnName(STRING_COLUMN).addSelect(selectColumns)
                        .addWindow(WindowType.NUM_ROWS, 1).build();

        ResultHandlerTest resultSet = new ResultHandlerTest();
        StreamingRead reader = new StreamingRead(sConnector, logicalWorkFlow, resultSet);

        String queryId = String.valueOf(Math.abs(random.nextLong()));
        reader.setQueryId(queryId);
        reader.start();
        waitSeconds(15);

        StreamingInserter streamingInserter = new StreamingInserter(sConnector, clusterName, tableMetadata,
                        new RowToInsertDefault());
        streamingInserter.numOfElement(10).elementPerSecond(5);
        streamingInserter.start();
        streamingInserter.addTypeToInsert(new ColumnType(DataType.VARCHAR));
        waitSeconds(10);

        streamingInserter.end();
        endReader(reader);

        ResultSet resSet = resultSet.getResultSet(queryId);

        List<ColumnMetadata> columnMetadata = resSet.getColumnMetadata();
        assertEquals(STRING_COLUMN, columnMetadata.get(0).getName().getName());

        // TODO double
        List<String> numsRecovered = new ArrayList<String>(10);
        for (Row recoveredRow : resSet.getRows()) {
            if (((String) recoveredRow.getCell(STRING_COLUMN).getValue()).equals("Text")) {
                numsRecovered.add((String) recoveredRow.getCell(STRING_COLUMN).getValue());
            }
        }

        assertTrue(columnMetadata.size() == 1);
        assertEquals(new ColumnType(DataType.VARCHAR), columnMetadata.get(0).getColumnType());
        assertTrue(resSet.getRows().get(0).getCell(STRING_COLUMN).getValue() instanceof String);

    }
}
