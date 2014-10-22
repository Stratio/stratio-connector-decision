/*
 * Licensed to STRATIO (C) under one or more contributor license agreements.
 *  See the NOTICE file distributed with this work for additional information
 *  regarding copyright ownership. The STRATIO (C) licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License. You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied. See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package com.stratio.connector.streaming.core.procces;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stratio.connector.commons.connection.Connection;
import com.stratio.connector.streaming.core.engine.query.ConnectorQueryBuilder;
import com.stratio.connector.streaming.core.engine.query.ConnectorQueryData;
import com.stratio.connector.streaming.core.engine.query.ConnectorQueryParser;
import com.stratio.connector.streaming.core.engine.query.queryexecutor.ConnectorQueryExecutor;
import com.stratio.connector.streaming.core.engine.query.util.StreamUtil;
import com.stratio.crossdata.common.connector.IResultHandler;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.exceptions.UnsupportedException;
import com.stratio.crossdata.common.logicalplan.Project;
import com.stratio.streaming.api.IStratioStreamingAPI;
import com.stratio.streaming.commons.exceptions.StratioAPISecurityException;
import com.stratio.streaming.commons.exceptions.StratioEngineOperationException;
import com.stratio.streaming.commons.exceptions.StratioEngineStatusException;

/**
 * Created by jmgomez on 3/10/14.
 */
public class QueryProcess implements ConnectorProcess {
    /**
     * The log.
     */
    private final transient Logger logger = LoggerFactory.getLogger(this.getClass());
    private String queryId;

    private Project project;
    private IResultHandler resultHandler;
    private Connection<IStratioStreamingAPI> connection;
    private ConnectorQueryExecutor queryExecutor;
    private ConnectorQueryParser queryParser = new ConnectorQueryParser();
    private ConnectorQueryBuilder queryBuilder = new ConnectorQueryBuilder();

    public QueryProcess(String queryId, Project project, IResultHandler resultHandler,
                    Connection<IStratioStreamingAPI> connection) {
        this.project = project;
        this.resultHandler = resultHandler;
        this.connection = connection;
        this.queryId = queryId;
    }

    public void run() {
        try {

            ConnectorQueryData queryData = queryParser.transformLogicalWorkFlow(project, queryId);

            String query = queryBuilder.createQuery(queryData);
            if (logger.isDebugEnabled()) {
                logger.debug("The streaming query is: [" + query + "]");

            }

            queryExecutor = new ConnectorQueryExecutor(queryData, resultHandler);
            queryExecutor.executeQuery(query, connection);

        } catch (StratioEngineStatusException | StratioAPISecurityException | StratioEngineOperationException
                        | UnsupportedException | ExecutionException e) {
            String msg = "Streaming query execution fail." + e.getMessage();
            logger.error(msg);
            resultHandler.processException(queryId, new ExecutionException(msg, e));

        } catch (InterruptedException e) {
            logger.info("The query is stopped");

        }
    }

    @Override
    public void endQuery() throws ExecutionException {

        try {

            queryExecutor.endQuery(StreamUtil.createStreamName(project.getTableName()), connection);
        } catch (StratioEngineStatusException | StratioAPISecurityException | StratioEngineOperationException e) {
            String msg = "Streaming query stop fail." + e.getMessage();
            logger.error(msg);
            throw new ExecutionException(msg, e);

        }
    }

    @Override
    public Project getProject() {

        return project;
    }

}
