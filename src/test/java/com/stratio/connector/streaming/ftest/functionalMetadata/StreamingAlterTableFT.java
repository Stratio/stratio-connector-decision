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
package com.stratio.connector.streaming.ftest.functionalMetadata;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.stratio.connector.commons.ftest.functionalMetadata.GenericMetadataAlterTableFT;
import com.stratio.connector.commons.ftest.helper.IConnectorHelper;
import com.stratio.connector.commons.metadata.TableMetadataBuilder;
import com.stratio.connector.commons.test.util.LogicalWorkFlowCreator;
import com.stratio.connector.streaming.core.StreamingConnector;
import com.stratio.connector.streaming.ftest.helper.StreamingConnectorHelper;
import com.stratio.connector.streaming.ftest.thread.actions.StreamingRead;
import com.stratio.crossdata.common.connector.IResultHandler;
import com.stratio.crossdata.common.data.AlterOperation;
import com.stratio.crossdata.common.data.AlterOptions;
import com.stratio.crossdata.common.data.Cell;
import com.stratio.crossdata.common.data.ClusterName;
import com.stratio.crossdata.common.data.ColumnName;
import com.stratio.crossdata.common.data.Row;
import com.stratio.crossdata.common.data.TableName;
import com.stratio.crossdata.common.exceptions.ConnectionException;
import com.stratio.crossdata.common.exceptions.ConnectorException;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.exceptions.InitializationException;
import com.stratio.crossdata.common.logicalplan.LogicalWorkflow;
import com.stratio.crossdata.common.metadata.ColumnMetadata;
import com.stratio.crossdata.common.metadata.ColumnType;
import com.stratio.crossdata.common.result.QueryResult;
import com.stratio.crossdata.common.statements.structures.window.WindowType;

public class StreamingAlterTableFT extends GenericMetadataAlterTableFT {

    @Override
    protected IConnectorHelper getConnectorHelper() {
        StreamingConnectorHelper sConnectorHelper = null;
        try {
            sConnectorHelper = new StreamingConnectorHelper(getClusterName());
            return sConnectorHelper;
        } catch (ConnectionException e) {
            e.printStackTrace();
        } catch (InitializationException e) {
            e.printStackTrace();
        }
        return sConnectorHelper;
    }

    @Before
    public void setUp() throws ConnectorException {
        iConnectorHelper = getConnectorHelper();
        connector = getConnector();
        connector.init(getConfiguration());
        connector.connect(getICredentials(), getConnectorClusterConfig());

        dropTable(CATALOG, TABLE);
        dropTable(CATALOG, TABLE + "_queryId");
        deleteCatalog(CATALOG);

    }

    @Override
    public void addColumnFT() throws ConnectorException {

        ClusterName clusterName = getClusterName();

        // Create the stream with COLUMN_1
        TableMetadataBuilder tableMetadataBuilder = new TableMetadataBuilder(CATALOG, TABLE);
        tableMetadataBuilder.addColumn(COLUMN_1, ColumnType.VARCHAR);

        connector.getMetadataEngine().createTable(clusterName, tableMetadataBuilder.build(false));
        sleep(5000);

        // ADD the column: COLUMN_2 with alterTable
        ColumnMetadata columnMetadata = new ColumnMetadata(new ColumnName(CATALOG, TABLE, COLUMN_2), new Object[0],
                        ColumnType.INT);
        AlterOptions alterOptions = new AlterOptions(AlterOperation.ADD_COLUMN, null, columnMetadata);
        connector.getMetadataEngine().alterTable(clusterName, new TableName(CATALOG, TABLE), alterOptions);

        sleep(5000);

        // Add the query to verify if the right column field is returned

        LogicalWorkFlowCreator logicalWorkFlowCreator = new LogicalWorkFlowCreator(CATALOG, TABLE, getClusterName());

        LinkedList<LogicalWorkFlowCreator.ConnectorField> selectColumns = new LinkedList<>();
        selectColumns.add(logicalWorkFlowCreator.createConnectorField(COLUMN_1, COLUMN_1, ColumnType.VARCHAR));
        selectColumns.add(logicalWorkFlowCreator.createConnectorField(COLUMN_2, COLUMN_2, ColumnType.INT));
        LogicalWorkflow lw = logicalWorkFlowCreator.addColumnName(COLUMN_1).addColumnName(COLUMN_2)
                        .addSelect(selectColumns).addWindow(WindowType.NUM_ROWS, 1).getLogicalWorkflow();

        StreamingResultHandler strResultHandler = new StreamingResultHandler();
        StreamingRead streamingReader = new StreamingRead((StreamingConnector) connector, lw, strResultHandler);
        streamingReader.setQueryId(UUID.randomUUID().toString());
        streamingReader.start();

        sleep(10000);

        // Insert a row with with column2 != null
        Row row = new Row();
        Map<String, Cell> cells = new HashMap<>();
        cells.put(COLUMN_1, new Cell("value1"));
        cells.put(COLUMN_2, new Cell(25));
        row.setCells(cells);

        tableMetadataBuilder.addColumn(COLUMN_2, ColumnType.INT);
        connector.getStorageEngine().insert(clusterName, tableMetadataBuilder.build(false), row,false);

        sleep(12000);

        QueryResult queryResult = strResultHandler.queryResult.get(0);

        if (strResultHandler.queryResult.size() > 1) {
            for (Row r : strResultHandler.queryResult.get(1).getResultSet().getRows()) {
                queryResult.getResultSet().add(r);
            }
        }

        Assert.assertTrue((queryResult.getResultSet().size() == 1) || (queryResult.getResultSet().size() == 2));
        assertEquals("Table [" + CATALOG + "." + TABLE + "] ", 1, queryResult.getResultSet().size());

        Row row2 = (queryResult.getResultSet().size() == 1) ? queryResult.getResultSet().getRows().get(0) : queryResult
                        .getResultSet().getRows().get(1);

        assertEquals(25, row2.getCell(COLUMN_2).getValue());

    }

    @Override
    @Test
    @Ignore
    public void dropColumnFT() throws ConnectorException {
        // TODO Auto-generated method stub
        super.dropColumnFT();
    }

    private void sleep(long miliseconds) {
        try {
            Thread.sleep(miliseconds);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}

class StreamingResultHandler implements IResultHandler {
    // boolean assertsMatch;
    // String column;
    // String value;
    List<QueryResult> queryResult;

    // public StreamingResultHandler(String column, Object value) {
    // assertsMatch=false;
    // // TODO Auto-generated constructor stub
    // }

    public StreamingResultHandler() {
        queryResult = new ArrayList<>();
    }

    @Override
    public void processException(String queryId, ExecutionException exception) {
        throw new RuntimeException("An exception has been launched by the connector");

    }

    @Override
    public void processResult(QueryResult result) {
        queryResult.add(result);
    }

}