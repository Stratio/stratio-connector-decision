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

import com.stratio.connector.commons.connection.Connection;
import com.stratio.connector.commons.connection.ConnectionHandler;
import com.stratio.connector.commons.engine.UniqueProjectQueryEngine;
import com.stratio.connector.streaming.core.QueryManager;
import com.stratio.connector.streaming.core.engine.query.ConnectorQueryBuilder;
import com.stratio.connector.streaming.core.engine.query.ConnectorQueryData;
import com.stratio.connector.streaming.core.engine.query.ConnectorQueryExecutor;
import com.stratio.connector.streaming.core.engine.query.ConnectorQueryParser;
import com.stratio.meta.common.exceptions.ExecutionException;
import com.stratio.meta.common.exceptions.UnsupportedException;
import com.stratio.meta.common.logicalplan.Project;
import com.stratio.meta.common.result.QueryResult;
import com.stratio.streaming.api.IStratioStreamingAPI;
import com.stratio.streaming.commons.exceptions.StratioAPISecurityException;
import com.stratio.streaming.commons.exceptions.StratioEngineOperationException;
import com.stratio.streaming.commons.exceptions.StratioEngineStatusException;

public class StreamingQueryEngine extends UniqueProjectQueryEngine<IStratioStreamingAPI> {

    public StreamingQueryEngine(ConnectionHandler connectionHandler) {
        super(connectionHandler);
    }

    /**
     * The log.
     */
    final Logger logger = LoggerFactory.getLogger(this.getClass());

    private ConnectorQueryParser queryParser = new ConnectorQueryParser();

    private ConnectorQueryBuilder queryBuilder;
    private ConnectorQueryExecutor queryExecutor = new ConnectorQueryExecutor();

    private QueryManager queryManager;

    public StreamingQueryEngine(ConnectionHandler connectionHandler, QueryManager queryManager) {
        super(connectionHandler);
        this.queryManager = queryManager;
    }

    @Override
    protected QueryResult execute(Project project, Connection<IStratioStreamingAPI> connection)
                    throws UnsupportedException, ExecutionException {
        try {

            ConnectorQueryData queryData = queryParser.transformLogicalWorkFlow(project);
            queryBuilder = new ConnectorQueryBuilder(queryData);
            String query = queryBuilder.createQuery();
            if (logger.isDebugEnabled()) {
                logger.debug("The streaming query is: [" + query + "]");
            }
            String streamingId = queryExecutor.executeQuery(query, connection, queryData);

            mappedMetaQueryIdStreamingQueryId(project, streamingId);

        } catch (StratioEngineStatusException | StratioAPISecurityException | StratioEngineOperationException e) {
            String msg = "Streaming query execution fail." + e.getMessage();
            logger.error(msg);
            throw new ExecutionException(msg, e);
        }

        throw new UnsupportedException("execute not supported in Streaming connector");
    }

    private void mappedMetaQueryIdStreamingQueryId(Project project, String streamingId) throws ExecutionException {
        queryManager.addQuery(getQueryId(project), streamingId);
    }

    private String getQueryId(Project project) {
        return "01234"; // TODO
    }
}
