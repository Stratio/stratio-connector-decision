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

import java.util.Collection;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stratio.connector.commons.connection.Connection;
import com.stratio.connector.commons.connection.exceptions.HandlerConnectionException;
import com.stratio.connector.commons.engine.CommonsStorageEngine;
import com.stratio.connector.elasticsearch.core.connection.ElasticSearchConnectionHandler;
import com.stratio.connector.elasticsearch.core.engine.utils.IndexRequestBuilderCreator;
import com.stratio.meta.common.data.Row;
import com.stratio.meta.common.exceptions.ExecutionException;
import com.stratio.meta.common.exceptions.UnsupportedException;
import com.stratio.meta2.common.metadata.TableMetadata;

/**
 * This class performs operations insert and delete in Elasticsearch.
 */

public class StreamingStorageEngine extends CommonsStorageEngine {

    /**
     * The Log.
     */
    final Logger logger = LoggerFactory.getLogger(this.getClass());
    /**
     * The index creator builder.
     */
    private IndexRequestBuilderCreator indexRequestBuilderCreator = new IndexRequestBuilderCreator();

    /**
     * Constructor.
     *
     * @param connectionHandler the connection handler.
     */
    public StreamingStorageEngine(ElasticSearchConnectionHandler connectionHandler) {

        super(connectionHandler);
    }

    /**
     * Insert a document in Elasticsearch.
     *
     *
     * @param targetTable   the targetName.
     * @param row           the row.
     * @throws com.stratio.meta.common.exceptions.ExecutionException   in case of failure during the execution.
     * @throws com.stratio.meta.common.exceptions.UnsupportedException it the operation is not supported.
     */

    @Override
    protected void insert(TableMetadata targetTable, Row row, Connection connection)
            throws UnsupportedException, ExecutionException {

        try {

            IndexRequestBuilder indexRequestBuilder = createIndexRequest(targetTable, row, connection);
            indexRequestBuilder.execute().actionGet();

            loggInsert(targetTable);

        } catch (HandlerConnectionException e) {

            throwHandlerException(e, "insert");

        }

    }

    /**
     * Insert a set of documents in Elasticsearch.
     *
     *
     * @param rows        the set of rows.
     * @throws com.stratio.meta.common.exceptions.ExecutionException   in case of failure during the execution.
     * @throws com.stratio.meta.common.exceptions.UnsupportedException if the operation is not supported.
     */
    protected void insert( TableMetadata targetTable, Collection<Row> rows,
            Connection connection) throws UnsupportedException, ExecutionException {

        try {
            BulkRequestBuilder bulkRequest = createBulkRequest(targetTable, rows, connection);

            BulkResponse bulkResponse = bulkRequest.execute().actionGet();

            validateBulkResponse(bulkResponse);

            logBulkInsert(targetTable, rows);

        } catch (HandlerConnectionException e) {
            throwHandlerException(e, "insert bulk");
        }

    }

    private IndexRequestBuilder createIndexRequest(TableMetadata targetTable, Row row,
            Connection connection) throws HandlerConnectionException, UnsupportedException {

        Client client = (Client) connection.getNativeConnection();

        return indexRequestBuilderCreator.createIndexRequestBuilder(targetTable, client, row);
    }

    private BulkRequestBuilder createBulkRequest(TableMetadata targetTable,
            Collection<Row> rows, Connection connection) throws HandlerConnectionException, UnsupportedException {

        Client elasticClient = (Client) connection.getNativeConnection();

        BulkRequestBuilder bulkRequest = elasticClient.prepareBulk();

        int i = 0;
        for (Row row : rows) {
            IndexRequestBuilder indexRequestBuilder = indexRequestBuilderCreator
                    .createIndexRequestBuilder(targetTable, elasticClient, row);
            bulkRequest.add(indexRequestBuilder);
            ;
        }
        return bulkRequest;
    }

    private void validateBulkResponse(BulkResponse bulkResponse) throws ExecutionException {
        if (bulkResponse.hasFailures()) {
            throw new ExecutionException(bulkResponse.buildFailureMessage());
        }
    }

    private void loggInsert(TableMetadata targetTable) {
        if (logger.isDebugEnabled()) {
            String index = targetTable.getName().getCatalogName().getName();
            String type = targetTable.getName().getName();
            logger.debug("Insert one row in ElasticSearch Database. Index [" + index + "] Type [" + type + "]");
        }
    }

    private void logBulkInsert(TableMetadata targetTable, Collection<Row> rows) {
        if (logger.isDebugEnabled()) {
            String index = targetTable.getName().getCatalogName().getName();
            String type = targetTable.getName().getName();
            logger.debug(
                    "Insert " + rows.size() + "  rows in ElasticSearch Database. Index [" + index + "] Type [" + type
                            + "]");
        }
    }

    private void throwHandlerException(HandlerConnectionException e, String method) throws ExecutionException {
        String exceptionMessage = "Fail Connecting elasticSearch in " + method + " method. " + e.getMessage();
        logger.error(exceptionMessage);
        throw new ExecutionException(exceptionMessage, e);
    }

}



