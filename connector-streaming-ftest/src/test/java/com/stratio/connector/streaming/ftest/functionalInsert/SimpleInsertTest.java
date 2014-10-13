/**
 * Copyright (C) 2014 Stratio (http://stratio.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.stratio.connector.streaming.ftest.functionalInsert;

import java.util.LinkedList;
import java.util.UUID;

import org.junit.Test;

import com.stratio.connector.commons.ftest.schema.TableMetadataBuilder;
import com.stratio.connector.commons.ftest.workFlow.LogicalWorkFlowCreator;
import com.stratio.connector.streaming.ftest.GenericStreamingTest;
import com.stratio.connector.streaming.ftest.TestResultSet;
import com.stratio.connector.streaming.ftest.thread.actions.StreamingInserter;
import com.stratio.connector.streaming.ftest.thread.actions.StreamingRead;
import com.stratio.meta.common.data.ResultSet;
import com.stratio.meta.common.exceptions.ExecutionException;
import com.stratio.meta.common.exceptions.UnsupportedException;
import com.stratio.meta.common.logicalplan.LogicalWorkflow;
import com.stratio.meta2.common.data.ClusterName;
import com.stratio.meta2.common.metadata.ColumnType;
import com.stratio.meta2.common.metadata.TableMetadata;

/**
 * Created by jmgomez on 16/07/14.
 */
public class SimpleInsertTest extends GenericStreamingTest {

    @Test
    public void testInsertInt() throws UnsupportedException, ExecutionException {
        ClusterName clusterName = getClusterName();
        System.out.println("*********************************** INIT FUNCTIONAL TEST testInsertInt "
                        + clusterName.getName() + " ***********************************");

        TableMetadataBuilder tableMetadataBuilder = new TableMetadataBuilder(CATALOG, TABLE);
        TableMetadata tableMetadata = tableMetadataBuilder.addColumn(INTEGER_COLUMN, ColumnType.INT).build();

        LogicalWorkFlowCreator logicalWorkFlowCreator = new LogicalWorkFlowCreator(CATALOG, TABLE, clusterName);
        LinkedList<LogicalWorkFlowCreator.ConnectorField> selectColumns = new LinkedList<>();
        selectColumns.add(logicalWorkFlowCreator.createConnectorField(INTEGER_COLUMN, INTEGER_COLUMN, ColumnType.INT));
        LogicalWorkflow logicalWorkFlow = logicalWorkFlowCreator.addColumnName(INTEGER_COLUMN).addSelect(selectColumns)
                        .getLogicalWorkflow();

        TestResultSet resultSet = new TestResultSet();
        StreamingRead reader = new StreamingRead(sConnector, clusterName, tableMetadata, logicalWorkFlow, resultSet);
        String queryId = UUID.randomUUID().toString();
        reader.setQueryId(queryId);
        reader.start();
        waitSeconds(5);

        StreamingInserter streamingInserter = new StreamingInserter(sConnector, clusterName, tableMetadata);
        streamingInserter.start();
        waitSeconds(5);

        streamingInserter.end();
        waitSeconds(5);
        reader.end();

        ResultSet resSet = resultSet.getResultSet(queryId);

    }

}
