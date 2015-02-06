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

package com.stratio.connector.streaming.query.window.theadHelper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stratio.connector.streaming.core.StreamingConnector;
import com.stratio.crossdata.common.connector.IResultHandler;
import com.stratio.crossdata.common.logicalplan.LogicalWorkflow;

public class StreamingThreadRead extends Thread {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    private StreamingConnector streamingConnector;
    private LogicalWorkflow logicalWorkFlow;
    private IResultHandler resultHandler;
    private String queryId;

    public StreamingThreadRead(StreamingConnector sC, LogicalWorkflow logicalWorkFlow, IResultHandler resultHandler) {
        super("[StreamingRead]");
        this.streamingConnector = sC;
        this.logicalWorkFlow = logicalWorkFlow;
        this.resultHandler = resultHandler;
        queryId = "queryId";
    }

    public void setQueryId(String queryId) {
        this.queryId = queryId;
    }

    public void run() {
        try {
            logger.debug("****************************** STARTING StreamingReader **********************");
            streamingConnector.getQueryEngine().asyncExecute(queryId, logicalWorkFlow, resultHandler);
        } catch (com.stratio.crossdata.common.exceptions.ConnectorException e) {
            logger.error("Error happens in Streaming Query." +e.toString());
            throw new RuntimeException(e);
        }
    }

    public void end() {
        try {
            streamingConnector.getQueryEngine().stop(queryId);
        } catch (com.stratio.crossdata.common.exceptions.ConnectorException e) {

            logger.error("Error happens in Streaming close." +e.toString());
            throw new RuntimeException(e);
        }
    }

}
