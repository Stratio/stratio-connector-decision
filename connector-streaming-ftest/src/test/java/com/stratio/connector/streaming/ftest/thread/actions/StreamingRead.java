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

package com.stratio.connector.streaming.ftest.thread.actions;

import com.stratio.connector.streaming.core.StreamingConnector;
import com.stratio.crossdata.common.connector.IResultHandler;
import com.stratio.crossdata.common.data.ClusterName;
import com.stratio.crossdata.common.logicalplan.LogicalWorkflow;
import com.stratio.crossdata.common.metadata.TableMetadata;

public class StreamingRead extends Thread {

    private StreamingConnector streamingConnector;
    private ClusterName clusterName;
    private TableMetadata tableMetadata;
    private LogicalWorkflow logicalWorkFlow;
    private IResultHandler resultHandler;
    private String queryId;

    public StreamingRead(StreamingConnector sC, ClusterName clusterName, TableMetadata tableMetadata,
            LogicalWorkflow logicalWorkFlow, IResultHandler resultHandler) {
        super("[StreamingRead]");
        this.streamingConnector = sC;
        this.clusterName = clusterName;
        this.tableMetadata = tableMetadata;
        this.logicalWorkFlow = logicalWorkFlow;
        this.resultHandler = resultHandler;
        queryId = "queryId";
    }

    public void setQueryId(String queryId) {
        this.queryId = queryId;
    }

    public void run() {
        try {
            System.out.println("****************************** STARTING StreamingInserter **********************");
            streamingConnector.getQueryEngine().asyncExecute(queryId, logicalWorkFlow, resultHandler);
        } catch (com.stratio.crossdata.common.exceptions.ConnectorException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public void end() {
        try {
            streamingConnector.getQueryEngine().stop(queryId);
        } catch (com.stratio.crossdata.common.exceptions.ConnectorException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

}
