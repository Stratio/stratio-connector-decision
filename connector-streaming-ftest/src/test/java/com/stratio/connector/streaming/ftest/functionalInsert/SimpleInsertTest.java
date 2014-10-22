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

import org.junit.Test;

import com.stratio.connector.commons.ftest.schema.TableMetadataBuilder;
import com.stratio.connector.commons.ftest.workFlow.LogicalWorkFlowCreator;
import com.stratio.connector.streaming.ftest.GenericStreamingTest;
import com.stratio.connector.streaming.ftest.TestResultSet;
import com.stratio.connector.streaming.ftest.thread.actions.StreamingInserter;
import com.stratio.connector.streaming.ftest.thread.actions.StreamingRead;
import com.stratio.crossdata.common.data.ClusterName;
import com.stratio.crossdata.common.data.ResultSet;
import com.stratio.crossdata.common.data.Row;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.exceptions.UnsupportedException;
import com.stratio.crossdata.common.logicalplan.LogicalWorkflow;
import com.stratio.crossdata.common.metadata.ColumnType;
import com.stratio.crossdata.common.metadata.TableMetadata;
import com.stratio.crossdata.common.metadata.structures.ColumnMetadata;

/**
 * @author david
 *
 */
public class SimpleInsertTest extends GenericStreamingTest {

    @Test
    public void testInsertInt() throws UnsupportedException, ExecutionException {
        ClusterName clusterName = getClusterName();
        System.out.println("*********************************** INIT FUNCTIONAL TEST testInsertInt "
                        + clusterName.getName() + " ***********************************");

        TableMetadataBuilder tableMetadataBuilder = new TableMetadataBuilder(CATALOG, TABLE);
        TableMetadata tableMetadata = tableMetadataBuilder.addColumn(INTEGER_COLUMN, ColumnType.INT)
                        .addColumn(STRING_COLUMN, ColumnType.VARCHAR).build();

        sConnector.getMetadataEngine().createTable(getClusterName(), tableMetadata);

        // TODO window with 1element
        LogicalWorkFlowCreator logicalWorkFlowCreator = new LogicalWorkFlowCreator(CATALOG, TABLE, clusterName);
        LinkedList<LogicalWorkFlowCreator.ConnectorField> selectColumns = new LinkedList<>();
        selectColumns.add(logicalWorkFlowCreator.createConnectorField(INTEGER_COLUMN, INTEGER_COLUMN, ColumnType.INT));
        selectColumns.add(logicalWorkFlowCreator.createConnectorField(STRING_COLUMN, STRING_COLUMN, ColumnType.VARCHAR));
        LogicalWorkflow logicalWorkFlow = logicalWorkFlowCreator.addColumnName(INTEGER_COLUMN)
                        .addColumnName(STRING_COLUMN).addSelect(selectColumns).getLogicalWorkflow();

        TestResultSet resultSet = new TestResultSet();
        StreamingRead reader = new StreamingRead(sConnector, clusterName, tableMetadata, logicalWorkFlow, resultSet);

        String queryId = String.valueOf(Math.abs(random.nextLong()));
        reader.setQueryId(queryId);
        reader.start();
        waitSeconds(15);

        StreamingInserter streamingInserter = new StreamingInserter(sConnector, clusterName, tableMetadata);
        streamingInserter.numOfElement(10).elementPerSecond(5);
        streamingInserter.start();
        streamingInserter.addTypeToInsert(ColumnType.INT).addTypeToInsert(ColumnType.VARCHAR);
        waitSeconds(10);

        streamingInserter.end();
        waitSeconds(15);
        reader.end();

        ResultSet resSet = resultSet.getResultSet(queryId);

        // TODO double
        List<Double> numsRecovered = new ArrayList<Double>(10);
        for (Row recoveredRow : resSet.getRows()) {
            if (recoveredRow.getCell(STRING_COLUMN).getValue().equals("Text")) {
                numsRecovered.add((Double) recoveredRow.getCell(INTEGER_COLUMN).getValue());
            }
        }

        assertEquals(10, numsRecovered.size());

        List<ColumnMetadata> columnMetadata = resSet.getColumnMetadata();
        assertEquals(INTEGER_COLUMN, columnMetadata.get(0).getColumnName());

        for (int i = 0; i < 10; i++) {
            assertTrue(numsRecovered.contains(new Double(i)));
        }

        assertTrue(columnMetadata.size() == 2);
        assertEquals(ColumnType.DOUBLE, columnMetadata.get(0).getType());
        assertTrue(resSet.getRows().get(0).getCell(INTEGER_COLUMN).getValue() instanceof Double);

    }

    @Test
    public void testInsertLong() throws UnsupportedException, ExecutionException {
        ClusterName clusterName = getClusterName();
        System.out.println("*********************************** INIT FUNCTIONAL TEST testInsertLong "
                        + clusterName.getName() + " ***********************************");

        TableMetadataBuilder tableMetadataBuilder = new TableMetadataBuilder(CATALOG, TABLE);
        TableMetadata tableMetadata = tableMetadataBuilder.addColumn(LONG_COLUMN, ColumnType.BIGINT)
                        .addColumn(STRING_COLUMN, ColumnType.VARCHAR).build();

        sConnector.getMetadataEngine().createTable(getClusterName(), tableMetadata);

        // TODO window with 1element
        LogicalWorkFlowCreator logicalWorkFlowCreator = new LogicalWorkFlowCreator(CATALOG, TABLE, clusterName);
        LinkedList<LogicalWorkFlowCreator.ConnectorField> selectColumns = new LinkedList<>();
        selectColumns.add(logicalWorkFlowCreator.createConnectorField(LONG_COLUMN, LONG_COLUMN, ColumnType.BIGINT));
        selectColumns.add(logicalWorkFlowCreator.createConnectorField(STRING_COLUMN, STRING_COLUMN, ColumnType.VARCHAR));
        LogicalWorkflow logicalWorkFlow = logicalWorkFlowCreator.addColumnName(LONG_COLUMN)
                        .addColumnName(STRING_COLUMN).addSelect(selectColumns).getLogicalWorkflow();

        TestResultSet resultSet = new TestResultSet();
        StreamingRead reader = new StreamingRead(sConnector, clusterName, tableMetadata, logicalWorkFlow, resultSet);

        String queryId = String.valueOf(Math.abs(random.nextLong()));
        reader.setQueryId(queryId);
        reader.start();
        waitSeconds(15);

        StreamingInserter streamingInserter = new StreamingInserter(sConnector, clusterName, tableMetadata);
        streamingInserter.numOfElement(10).elementPerSecond(5);
        streamingInserter.start();
        streamingInserter.addTypeToInsert(ColumnType.BIGINT).addTypeToInsert(ColumnType.VARCHAR);
        ;
        waitSeconds(10);

        streamingInserter.end();
        waitSeconds(15);
        reader.end();

        ResultSet resSet = resultSet.getResultSet(queryId);

        List<ColumnMetadata> columnMetadata = resSet.getColumnMetadata();
        assertEquals(LONG_COLUMN, columnMetadata.get(0).getColumnName());

        // TODO double
        List<Double> numsRecovered = new ArrayList<Double>(10);
        for (Row recoveredRow : resSet.getRows()) {
            if (recoveredRow.getCell(STRING_COLUMN).getValue().equals("Text")) {
                numsRecovered.add((Double) recoveredRow.getCell(LONG_COLUMN).getValue());
            }
        }

        assertEquals(10, numsRecovered.size());

        for (int i = 0; i < 10; i++) {
            assertTrue(numsRecovered.contains(new Double(new Long(i + Long.MAX_VALUE / 2))));
        }

        assertTrue(columnMetadata.size() == 2);
        assertEquals(ColumnType.DOUBLE, columnMetadata.get(0).getType());
        assertTrue(resSet.getRows().get(0).getCell(LONG_COLUMN).getValue() instanceof Double);

    }

    @Test
    public void testInsertBoolean() throws UnsupportedException, ExecutionException {
        ClusterName clusterName = getClusterName();
        System.out.println("*********************************** INIT FUNCTIONAL TEST testInsertBool "
                        + clusterName.getName() + " ***********************************");

        TableMetadataBuilder tableMetadataBuilder = new TableMetadataBuilder(CATALOG, TABLE);
        TableMetadata tableMetadata = tableMetadataBuilder.addColumn(BOOLEAN_COLUMN, ColumnType.BOOLEAN)
                        .addColumn(STRING_COLUMN, ColumnType.VARCHAR).build();

        sConnector.getMetadataEngine().createTable(getClusterName(), tableMetadata);

        // TODO window with 1element
        LogicalWorkFlowCreator logicalWorkFlowCreator = new LogicalWorkFlowCreator(CATALOG, TABLE, clusterName);
        LinkedList<LogicalWorkFlowCreator.ConnectorField> selectColumns = new LinkedList<>();
        selectColumns.add(logicalWorkFlowCreator.createConnectorField(BOOLEAN_COLUMN, BOOLEAN_COLUMN,
                        ColumnType.BOOLEAN));
        selectColumns.add(logicalWorkFlowCreator.createConnectorField(STRING_COLUMN, STRING_COLUMN, ColumnType.VARCHAR));
        LogicalWorkflow logicalWorkFlow = logicalWorkFlowCreator.addColumnName(BOOLEAN_COLUMN, STRING_COLUMN)
                        .addSelect(selectColumns).getLogicalWorkflow();

        TestResultSet resultSet = new TestResultSet();
        StreamingRead reader = new StreamingRead(sConnector, clusterName, tableMetadata, logicalWorkFlow, resultSet);

        String queryId = String.valueOf(Math.abs(random.nextLong()));
        reader.setQueryId(queryId);
        reader.start();
        waitSeconds(15);

        StreamingInserter streamingInserter = new StreamingInserter(sConnector, clusterName, tableMetadata);
        streamingInserter.numOfElement(10).elementPerSecond(5);
        streamingInserter.start();
        streamingInserter.addTypeToInsert(ColumnType.BOOLEAN).addTypeToInsert(ColumnType.VARCHAR);
        waitSeconds(10);

        streamingInserter.end();
        waitSeconds(15);
        reader.end();

        ResultSet resSet = resultSet.getResultSet(queryId);

        List<ColumnMetadata> columnMetadata = resSet.getColumnMetadata();
        assertEquals(BOOLEAN_COLUMN, columnMetadata.get(0).getColumnName());

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
        assertEquals(ColumnType.BOOLEAN, columnMetadata.get(0).getType());
        assertTrue(resSet.getRows().get(0).getCell(BOOLEAN_COLUMN).getValue() instanceof Boolean);
    }

    @Test
    public void testInsertFloat() throws UnsupportedException, ExecutionException {
        ClusterName clusterName = getClusterName();
        System.out.println("*********************************** INIT FUNCTIONAL TEST testInsertFloat "
                        + clusterName.getName() + " ***********************************");

        TableMetadataBuilder tableMetadataBuilder = new TableMetadataBuilder(CATALOG, TABLE);
        TableMetadata tableMetadata = tableMetadataBuilder.addColumn(FLOAT_COLUMN, ColumnType.FLOAT)
                        .addColumn(STRING_COLUMN, ColumnType.VARCHAR).build();

        sConnector.getMetadataEngine().createTable(getClusterName(), tableMetadata);

        // TODO window with 1element
        LogicalWorkFlowCreator logicalWorkFlowCreator = new LogicalWorkFlowCreator(CATALOG, TABLE, clusterName);
        LinkedList<LogicalWorkFlowCreator.ConnectorField> selectColumns = new LinkedList<>();
        selectColumns.add(logicalWorkFlowCreator.createConnectorField(FLOAT_COLUMN, FLOAT_COLUMN, ColumnType.FLOAT));
        selectColumns.add(logicalWorkFlowCreator.createConnectorField(STRING_COLUMN, STRING_COLUMN, ColumnType.VARCHAR));
        LogicalWorkflow logicalWorkFlow = logicalWorkFlowCreator.addColumnName(FLOAT_COLUMN, STRING_COLUMN)
                        .addSelect(selectColumns).getLogicalWorkflow();

        TestResultSet resultSet = new TestResultSet();
        StreamingRead reader = new StreamingRead(sConnector, clusterName, tableMetadata, logicalWorkFlow, resultSet);

        String queryId = String.valueOf(Math.abs(random.nextLong()));
        reader.setQueryId(queryId);
        reader.start();
        waitSeconds(15);

        StreamingInserter streamingInserter = new StreamingInserter(sConnector, clusterName, tableMetadata);
        streamingInserter.numOfElement(10).elementPerSecond(5);
        streamingInserter.start();
        streamingInserter.addTypeToInsert(ColumnType.FLOAT).addTypeToInsert(ColumnType.VARCHAR);
        waitSeconds(10);

        streamingInserter.end();
        waitSeconds(15);
        reader.end();

        ResultSet resSet = resultSet.getResultSet(queryId);

        List<ColumnMetadata> columnMetadata = resSet.getColumnMetadata();
        assertEquals(FLOAT_COLUMN, columnMetadata.get(0).getColumnName());

        // TODO double
        List<Double> numsRecovered = new ArrayList<Double>(10);
        for (Row recoveredRow : resSet.getRows()) {
            if (recoveredRow.getCell(STRING_COLUMN).getValue().equals("Text")) {
                numsRecovered.add((Double) recoveredRow.getCell(FLOAT_COLUMN).getValue());
            }
        }

        assertEquals(10, numsRecovered.size());

        for (int i = 0; i < 10; i++) {
            assertTrue(numsRecovered.contains(new Double(i + 0.5)));
        }

        assertTrue(columnMetadata.size() == 2);
        assertEquals(ColumnType.DOUBLE, columnMetadata.get(0).getType());
        assertTrue(resSet.getRows().get(0).getCell(FLOAT_COLUMN).getValue() instanceof Double);

    }

    @Test
    public void testInsertDouble() throws UnsupportedException, ExecutionException {
        ClusterName clusterName = getClusterName();
        System.out.println("*********************************** INIT FUNCTIONAL TEST testInsertDouble "
                        + clusterName.getName() + " ***********************************");

        TableMetadataBuilder tableMetadataBuilder = new TableMetadataBuilder(CATALOG, TABLE);
        TableMetadata tableMetadata = tableMetadataBuilder.addColumn(DOUBLE_COLUMN, ColumnType.DOUBLE)
                        .addColumn(STRING_COLUMN, ColumnType.VARCHAR).build();

        sConnector.getMetadataEngine().createTable(getClusterName(), tableMetadata);

        // TODO window with 1element
        LogicalWorkFlowCreator logicalWorkFlowCreator = new LogicalWorkFlowCreator(CATALOG, TABLE, clusterName);
        LinkedList<LogicalWorkFlowCreator.ConnectorField> selectColumns = new LinkedList<>();
        selectColumns.add(logicalWorkFlowCreator.createConnectorField(DOUBLE_COLUMN, DOUBLE_COLUMN, ColumnType.DOUBLE));
        selectColumns.add(logicalWorkFlowCreator.createConnectorField(STRING_COLUMN, STRING_COLUMN, ColumnType.VARCHAR));
        LogicalWorkflow logicalWorkFlow = logicalWorkFlowCreator.addColumnName(DOUBLE_COLUMN, STRING_COLUMN)
                        .addSelect(selectColumns).getLogicalWorkflow();

        TestResultSet resultSet = new TestResultSet();
        StreamingRead reader = new StreamingRead(sConnector, clusterName, tableMetadata, logicalWorkFlow, resultSet);

        String queryId = String.valueOf(Math.abs(random.nextLong()));
        reader.setQueryId(queryId);
        reader.start();
        waitSeconds(15);

        StreamingInserter streamingInserter = new StreamingInserter(sConnector, clusterName, tableMetadata);
        streamingInserter.numOfElement(10).elementPerSecond(5);
        streamingInserter.start();
        streamingInserter.addTypeToInsert(ColumnType.DOUBLE).addTypeToInsert(ColumnType.VARCHAR);
        waitSeconds(10);

        streamingInserter.end();
        waitSeconds(15);
        reader.end();

        ResultSet resSet = resultSet.getResultSet(queryId);

        List<ColumnMetadata> columnMetadata = resSet.getColumnMetadata();
        assertEquals(DOUBLE_COLUMN, columnMetadata.get(0).getColumnName());

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
        assertEquals(ColumnType.DOUBLE, columnMetadata.get(0).getType());
        assertTrue(resSet.getRows().get(0).getCell(DOUBLE_COLUMN).getValue() instanceof Double);

    }

    @Test
    public void testInsertString() throws UnsupportedException, ExecutionException {
        ClusterName clusterName = getClusterName();
        System.out.println("*********************************** INIT FUNCTIONAL TEST testInsertDouble "
                        + clusterName.getName() + " ***********************************");

        TableMetadataBuilder tableMetadataBuilder = new TableMetadataBuilder(CATALOG, TABLE);
        TableMetadata tableMetadata = tableMetadataBuilder.addColumn(STRING_COLUMN, ColumnType.VARCHAR).build();

        sConnector.getMetadataEngine().createTable(getClusterName(), tableMetadata);

        // TODO window with 1element
        LogicalWorkFlowCreator logicalWorkFlowCreator = new LogicalWorkFlowCreator(CATALOG, TABLE, clusterName);
        LinkedList<LogicalWorkFlowCreator.ConnectorField> selectColumns = new LinkedList<>();
        selectColumns.add(logicalWorkFlowCreator.createConnectorField(STRING_COLUMN, STRING_COLUMN, ColumnType.VARCHAR));
        LogicalWorkflow logicalWorkFlow = logicalWorkFlowCreator.addColumnName(STRING_COLUMN).addSelect(selectColumns)
                        .getLogicalWorkflow();

        TestResultSet resultSet = new TestResultSet();
        StreamingRead reader = new StreamingRead(sConnector, clusterName, tableMetadata, logicalWorkFlow, resultSet);

        String queryId = String.valueOf(Math.abs(random.nextLong()));
        reader.setQueryId(queryId);
        reader.start();
        waitSeconds(15);

        StreamingInserter streamingInserter = new StreamingInserter(sConnector, clusterName, tableMetadata);
        streamingInserter.numOfElement(10).elementPerSecond(5);
        streamingInserter.start();
        streamingInserter.addTypeToInsert(ColumnType.VARCHAR);
        waitSeconds(10);

        streamingInserter.end();
        waitSeconds(15);
        reader.end();

        ResultSet resSet = resultSet.getResultSet(queryId);

        assertEquals(10, resSet.size());

        List<ColumnMetadata> columnMetadata = resSet.getColumnMetadata();
        assertEquals(STRING_COLUMN, columnMetadata.get(0).getColumnName());

        // TODO double
        List<String> numsRecovered = new ArrayList<String>(10);
        for (Row recoveredRow : resSet.getRows()) {
            if (((String) recoveredRow.getCell(STRING_COLUMN).getValue()).equals("Text")) {
                numsRecovered.add((String) recoveredRow.getCell(STRING_COLUMN).getValue());
            }
        }

        assertTrue(columnMetadata.size() == 1);
        assertEquals(ColumnType.VARCHAR, columnMetadata.get(0).getType());
        assertTrue(resSet.getRows().get(0).getCell(STRING_COLUMN).getValue() instanceof String);

    }
}
