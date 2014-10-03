/*
 * Stratio Meta
 *
 *   Copyright (c) 2014, Stratio, All rights reserved.
 *
 *   This library is free software; you can redistribute it and/or modify it under the terms of the
 *   GNU Lesser General Public License as published by the Free Software Foundation; either version
 *   3.0 of the License, or (at your option) any later version.
 *
 *   This library is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 *   even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 *   Lesser General Public License for more details.
 *
 *   You should have received a copy of the GNU Lesser General Public License along with this library.
 */
package com.stratio.connector.streaming.core.engine;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import com.stratio.connector.commons.connection.ConnectionHandler;
import com.stratio.connector.commons.connection.exceptions.HandlerConnectionException;

import com.stratio.connector.streaming.core.procces.ConnectorProcessHandler;
import com.stratio.connector.streaming.core.procces.QueryProcess;
import com.stratio.connector.streaming.core.procces.exception.ConnectionProcessException;
import com.stratio.meta.common.connector.IQueryEngine;
import com.stratio.meta.common.connector.IResultHandler;
import com.stratio.meta.common.exceptions.ExecutionException;
import com.stratio.meta.common.exceptions.UnsupportedException;
import com.stratio.meta.common.logicalplan.LogicalWorkflow;
import com.stratio.meta.common.logicalplan.Project;
import com.stratio.meta.common.result.QueryResult;


public class StreamingQueryEngine implements IQueryEngine {


    private transient ConnectorProcessHandler connectorProcessHandler;
    private transient ConnectionHandler connectionHandler;


    public StreamingQueryEngine(ConnectionHandler connectionHandler,
            ConnectorProcessHandler processHandler) {

        this.connectionHandler = connectionHandler;
        this.connectorProcessHandler = processHandler;
    }

    @Override public QueryResult execute(LogicalWorkflow workflow) throws UnsupportedException, ExecutionException {
        throw new UnsupportedException("execute not supported in Streaming connector");
    }

    @Override public void asyncExecute(String queryId, LogicalWorkflow workflow, IResultHandler resultHandler)
            throws UnsupportedException, ExecutionException {
        checkExceptions(queryId, workflow, resultHandler);
        try {
            initProcess(queryId, workflow, resultHandler).run();

        } catch (ConnectionProcessException | HandlerConnectionException e) {
            resultHandler.processException(queryId,new ExecutionException("Fail process creation",e));
        }
    }



    @Override public void stop(String queryId) throws UnsupportedException, ExecutionException {
        try {
            connectorProcessHandler.getProcess(queryId).endQuery();
        } catch (ConnectionProcessException e) {
            throw new ExecutionException("Fail process stop",e);
        }
    }


    private QueryProcess initProcess(String queryId, LogicalWorkflow workflow, IResultHandler resultHandler)
            throws ConnectionProcessException, HandlerConnectionException {

        Project project = (Project) workflow.getInitialSteps().get(0);
        String clusterName = project.getClusterName().getName();
        connectionHandler.startWork(clusterName);
        QueryProcess queryProcess = new QueryProcess(queryId,project, resultHandler,
                connectionHandler.getConnection(clusterName));

        connectorProcessHandler.addProcess(queryId, queryProcess);

        return queryProcess;
    }

    private void checkExceptions(String queryId, LogicalWorkflow workflow, IResultHandler resultHandler) {
        if (workflow.getInitialSteps().size()!=1){
            resultHandler.processException(queryId,new ExecutionException("Only one project can be executed in " +
                    "Streaming"));
        }
    }


}
