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

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.client.Client;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stratio.connector.commons.connection.Connection;
import com.stratio.connector.commons.engine.UniqueProjectQueryEngine;
import com.stratio.connector.elasticsearch.core.connection.ElasticSearchConnectionHandler;
import com.stratio.connector.elasticsearch.core.engine.query.ConnectorQueryBuilder;
import com.stratio.connector.elasticsearch.core.engine.query.ConnectorQueryData;
import com.stratio.connector.elasticsearch.core.engine.query.ConnectorQueryExecutor;
import com.stratio.connector.elasticsearch.core.engine.query.ConnectorQueryParser;
import com.stratio.meta.common.exceptions.ExecutionException;
import com.stratio.meta.common.exceptions.UnsupportedException;
import com.stratio.meta.common.logicalplan.LogicalWorkflow;
import com.stratio.meta.common.result.QueryResult;

public class StreamingQueryEngine extends UniqueProjectQueryEngine {


    private ConnectorQueryParser queryParser = new ConnectorQueryParser();

    private ConnectorQueryBuilder queryBuilder = new ConnectorQueryBuilder();

    private ConnectorQueryExecutor queryExecutor = new ConnectorQueryExecutor();

    /**
     * The log.
     */
    final Logger logger = LoggerFactory.getLogger(this.getClass());

    public StreamingQueryEngine(ElasticSearchConnectionHandler connectionHandle) {

        super(connectionHandle);

    }




    private QueryResult execute(Client elasticClient, LogicalWorkflow logicalWorkFlow)
            throws UnsupportedException, ExecutionException {


        ConnectorQueryData queryData = queryParser.transformLogicalWorkFlow(logicalWorkFlow);

        SearchRequestBuilder requestBuilder = queryBuilder.buildQuery(elasticClient, queryData);

        QueryResult resultSet = queryExecutor.executeQuery(elasticClient, requestBuilder, queryData);

        return resultSet;

    }

    @Override
    protected QueryResult execute(LogicalWorkflow logicalWorkflow, Connection connection)
            throws UnsupportedException, ExecutionException {

                QueryResult  queryResult = execute((Client) connection.getNativeConnection(), logicalWorkflow);


        return queryResult;

    }
}
