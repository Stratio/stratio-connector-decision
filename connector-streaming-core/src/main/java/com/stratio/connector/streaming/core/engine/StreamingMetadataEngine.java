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
package com.stratio.connector.streaming.core.engine;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stratio.connector.commons.connection.Connection;
import com.stratio.connector.commons.connection.ConnectionHandler;
import com.stratio.connector.commons.engine.CommonsMetadataEngine;
import com.stratio.connector.streaming.core.engine.query.util.StreamUtil;
import com.stratio.crossdata.common.data.CatalogName;
import com.stratio.crossdata.common.data.ColumnName;
import com.stratio.crossdata.common.data.TableName;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.exceptions.UnsupportedException;
import com.stratio.crossdata.common.metadata.CatalogMetadata;
import com.stratio.crossdata.common.metadata.ColumnType;
import com.stratio.crossdata.common.metadata.IndexMetadata;
import com.stratio.crossdata.common.metadata.TableMetadata;
import com.stratio.streaming.api.IStratioStreamingAPI;
import com.stratio.streaming.api.messaging.ColumnNameType;
import com.stratio.streaming.commons.exceptions.StratioAPISecurityException;
import com.stratio.streaming.commons.exceptions.StratioEngineOperationException;
import com.stratio.streaming.commons.exceptions.StratioEngineStatusException;
import com.stratio.streaming.commons.exceptions.StratioStreamingException;

/**
 * This class is the responsible of manage the StreamingMetadata
 */

public class StreamingMetadataEngine extends CommonsMetadataEngine<IStratioStreamingAPI> {

    /**
     * The Log.
     */
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    /**
     * Constructor.
     *
     * @param connectionHandler the connector handle.
     */
    public StreamingMetadataEngine(ConnectionHandler connectionHandler) {
        super(connectionHandler);
    }

    /**
     * This method create a index in ES.
     *
     * @param indexMetaData the index configuration.
     * @throws com.stratio.meta.common.exceptions.UnsupportedException if any operation is not supported.
     * @throws com.stratio.meta.common.exceptions.ExecutionException   if an error occur.
     */

    @Override
    protected void createCatalog(CatalogMetadata indexMetaData, Connection<IStratioStreamingAPI> connection)
            throws UnsupportedException, ExecutionException {
        throw new UnsupportedException("Create catalog not supported in Streaming connector");
    }

    /**
     * This method create a type in Streaming.
     *
     * @param streamMetadata the stream configuration.
     * @throws com.stratio.meta.common.exceptions.UnsupportedException if any operation is not supported.
     * @throws com.stratio.meta.common.exceptions.ExecutionException   if an error occur.
     */
    @Override
    protected void createTable(TableMetadata streamMetadata, Connection<IStratioStreamingAPI> connection)
            throws UnsupportedException, ExecutionException {

        String streamName = StreamUtil.createStreamName(streamMetadata.getName());
        try {
            List<ColumnNameType> columnList = new ArrayList<ColumnNameType>();

            for (ColumnName columnInfo : streamMetadata.getColumns().keySet()) {
                String columnName = columnInfo.getName();
                com.stratio.streaming.commons.constants.ColumnType columnType = convertType(streamMetadata.getColumns()
                        .get(columnInfo).getColumnType());

                columnList.add(new ColumnNameType(columnName, columnType));
            }
            connection.getNativeConnection().createStream(streamName, columnList);
        } catch (StratioEngineOperationException | StratioEngineStatusException | StratioAPISecurityException e) {
            String msg = "Fail creating the Stream [" + streamName + "]. " + e.getMessage();
            logger.error(msg);
            throw new ExecutionException(msg, e);
        }

    }

    /**
     * This method drop a index in Streaming.
     *
     * @param indexName the index name.
     */

    @Override
    protected void dropCatalog(CatalogName indexName, Connection<IStratioStreamingAPI> connection)
            throws ExecutionException, UnsupportedException {
        throw new UnsupportedException("Drop catalog not supported in Streaming connector");

    }

    /**
     * This method drop a type in Streaming.
     *
     * @param stream the stream name.
     */
    @Override
    protected void dropTable(TableName stream, Connection<IStratioStreamingAPI> connection) throws ExecutionException,
            UnsupportedException {
        String streamName = StreamUtil.createStreamName(stream);
        try {

            connection.getNativeConnection().dropStream(streamName);
        } catch (StratioStreamingException e) {
            String msg = "Fail dropping the Stream [" + streamName + "]. " + e.getMessage();
            logger.error(msg);
            throw new ExecutionException(msg, e);
        }

    }

    @Override
    protected void createIndex(IndexMetadata indexMetadata, Connection<IStratioStreamingAPI> connection)
            throws UnsupportedException, ExecutionException {
        throw new UnsupportedException("Create Index not supported in Streaming connector");
    }

    @Override
    protected void dropIndex(IndexMetadata indexMetadata, Connection<IStratioStreamingAPI> connection)
            throws UnsupportedException, ExecutionException {
        throw new UnsupportedException("Drop Index not supported in Streaming connector");
    }

    private com.stratio.streaming.commons.constants.ColumnType convertType(ColumnType columnType)
            throws UnsupportedException {
        com.stratio.streaming.commons.constants.ColumnType returnType = null;
        switch (columnType) {

        case BIGINT:
            returnType = com.stratio.streaming.commons.constants.ColumnType.LONG;
            break;
        case BOOLEAN:
            returnType = com.stratio.streaming.commons.constants.ColumnType.BOOLEAN;
            break;
        case DOUBLE:
            returnType = com.stratio.streaming.commons.constants.ColumnType.DOUBLE;
            break;
        case FLOAT:
            returnType = com.stratio.streaming.commons.constants.ColumnType.FLOAT;
            break;
        case INT:
            returnType = com.stratio.streaming.commons.constants.ColumnType.INTEGER;
            break;
        case TEXT:
        case VARCHAR:
            returnType = com.stratio.streaming.commons.constants.ColumnType.STRING;
            break;
        default:
            throw new UnsupportedException("Column type " + columnType.name() + " not supported in Streaming");

        }
        return returnType;
    }

}
