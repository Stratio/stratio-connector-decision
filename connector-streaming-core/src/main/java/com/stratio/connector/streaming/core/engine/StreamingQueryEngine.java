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
package com.stratio.connector.streaming.core.engine;

import com.stratio.connector.commons.connection.ConnectionHandler;
import com.stratio.connector.commons.connection.exceptions.HandlerConnectionException;
import com.stratio.connector.streaming.core.procces.ConnectorProcess;
import com.stratio.connector.streaming.core.procces.ConnectorProcessHandler;
import com.stratio.connector.streaming.core.procces.QueryProcess;
import com.stratio.connector.streaming.core.procces.exception.ConnectionProcessException;
import com.stratio.crossdata.common.connector.IQueryEngine;
import com.stratio.crossdata.common.connector.IResultHandler;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.exceptions.UnsupportedException;
import com.stratio.crossdata.common.logicalplan.LogicalWorkflow;
import com.stratio.crossdata.common.logicalplan.Project;
import com.stratio.crossdata.common.result.QueryResult;

/**
 * This class is a Streaming implementation for queryEngine.
 */
public class StreamingQueryEngine implements IQueryEngine {

    /**
     * The processor. handler.
     */
    private transient ConnectorProcessHandler connectorProcessHandler;
    /**
     * The connection.
     */
    private transient ConnectionHandler connectionHandler;

    /**
     * Constructor.
     * @param connectionHandler the connection handler.
     * @param processHandler the processor handler.
     */
    public StreamingQueryEngine(ConnectionHandler connectionHandler, ConnectorProcessHandler processHandler) {

        this.connectionHandler = connectionHandler;
        this.connectorProcessHandler = processHandler;
    }

    /**
     * Execute a query.
     * @param workflow the work flow witch represents the query.
     * @return the query result.
     * @throws UnsupportedException if aany operation is not supported.
     * @throws ExecutionException if any error happens.
     */
    @Override
    public QueryResult execute(LogicalWorkflow workflow) throws UnsupportedException, ExecutionException {
        throw new UnsupportedException("execute not supported in Streaming connector");
    }


    /**
     * Execute a asynchronous query.
     * @param queryId the queryId.
     * @param workflow the work flow witch represents the query.
     * @param resultHandler the result handler.
     * @return the query result.
     * @throws UnsupportedException if aany operation is not supported.
     * @throws ExecutionException if any error happens.
     */
    @Override
    public void asyncExecute(String queryId, LogicalWorkflow workflow, IResultHandler resultHandler)
            throws UnsupportedException, ExecutionException {
        checkExceptions(queryId, workflow, resultHandler);
        try {
            connectorProcessHandler.strartProcess(queryId, initProcess(queryId, workflow, resultHandler));

        } catch (ConnectionProcessException | HandlerConnectionException e) {

            resultHandler.processException(queryId, new ExecutionException("Fail process creation", e));
        } finally {
            // TODO ensure to end all threads.

        }
    }

    /**
     * This method stop a query.
     * @param queryId the queryId.
     * @throws UnsupportedException if any operation is not supported.
     *
     * @throws ExecutionException if any error happens.
     */
    @Override
    public synchronized void stop(String queryId) throws UnsupportedException, ExecutionException {
        try {
            ConnectorProcess process = connectorProcessHandler.getProcess(queryId);
            connectionHandler.endWork(process.getProject().getClusterName().getName());
            connectorProcessHandler.stopProcess(queryId);
        } catch (ConnectionProcessException e) {
            throw new ExecutionException("Fail process stop", e);
        }
    }

    /**
     * This method initialize a process.
     * @param queryId the queryId.
     * @param workflow the workflow.
     * @param resultHandler the result handler.
     * @return a query process.
     * @throws ConnectionProcessException if the connection fails.
     * @throws HandlerConnectionException if handling the connection fails.
     */
    private QueryProcess initProcess(String queryId, LogicalWorkflow workflow, IResultHandler resultHandler)
            throws ConnectionProcessException, HandlerConnectionException {

        Project project = (Project) workflow.getInitialSteps().get(0);
        String clusterName = project.getClusterName().getName();
        connectionHandler.startWork(clusterName);
        QueryProcess queryProcess = new QueryProcess(queryId, project, resultHandler,
                connectionHandler.getConnection(clusterName));

        return queryProcess;
    }

    /**
     * check if a exception happens.
     * @param queryId the queryId.
     * @param workflow the workflow,.
     * @param resultHandler the resultHandler.
     */
    private void checkExceptions(String queryId, LogicalWorkflow workflow, IResultHandler resultHandler) {
        if (workflow.getInitialSteps().size() != 1) {
            resultHandler.processException(queryId, new ExecutionException("Only one project can be executed in "
                    + "Streaming"));
        }
    }

}
