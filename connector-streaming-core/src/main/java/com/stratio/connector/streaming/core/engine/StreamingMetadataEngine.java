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

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stratio.connector.commons.connection.Connection;
import com.stratio.connector.commons.connection.ConnectionHandler;
import com.stratio.connector.commons.engine.CommonsMetadataEngine;
import com.stratio.meta.common.exceptions.ExecutionException;
import com.stratio.meta.common.exceptions.UnsupportedException;
import com.stratio.meta2.common.data.CatalogName;
import com.stratio.meta2.common.data.ColumnName;
import com.stratio.meta2.common.data.TableName;
import com.stratio.meta2.common.metadata.CatalogMetadata;
import com.stratio.meta2.common.metadata.ColumnType;
import com.stratio.meta2.common.metadata.IndexMetadata;
import com.stratio.meta2.common.metadata.TableMetadata;
import com.stratio.streaming.api.IStratioStreamingAPI;
import com.stratio.streaming.api.messaging.ColumnNameType;
import com.stratio.streaming.commons.exceptions.StratioAPISecurityException;
import com.stratio.streaming.commons.exceptions.StratioEngineOperationException;
import com.stratio.streaming.commons.exceptions.StratioEngineStatusException;
import com.stratio.streaming.commons.exceptions.StratioStreamingException;

/**
 * This class is the responsible of manage the StreamingMetadata
 *
 */
public class StreamingMetadataEngine extends CommonsMetadataEngine<IStratioStreamingAPI> {

    /**
     * The Log.
     */
    final Logger logger = LoggerFactory.getLogger(this.getClass());

    /**
     * Constructor.
     *
     * @param connectionHandler
     *            the connector handle.
     */
    public StreamingMetadataEngine(ConnectionHandler connectionHandler) {
        super(connectionHandler);
    }

    /**
     * This method create a index in ES.
     *
     * 
     * @param indexMetaData
     *            the index configuration.
     * @throws com.stratio.meta.common.exceptions.UnsupportedException
     *             if any operation is not supported.
     * @throws com.stratio.meta.common.exceptions.ExecutionException
     *             if an error occur.
     */

    @Override
    protected void createCatalog(CatalogMetadata indexMetaData, Connection<IStratioStreamingAPI> connection)
                    throws UnsupportedException, ExecutionException {
        throw new UnsupportedException("Create catalog not supported in Streaming connector");
    }

    /**
     * This method create a type in Streaming.
     *
     *
     * @param streamMetadata
     *            the stream configuration.
     * @throws com.stratio.meta.common.exceptions.UnsupportedException
     *             if any operation is not supported.
     * @throws com.stratio.meta.common.exceptions.ExecutionException
     *             if an error occur.
     */
    @Override
    protected void createTable(TableMetadata streamMetadata, Connection<IStratioStreamingAPI> connection)
                    throws UnsupportedException, ExecutionException {
        String streamName = streamMetadata.getName().getCatalogName().getName() + "_"
                        + streamMetadata.getName().getName();
        try {
            List columnList = new ArrayList();

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
     * 
     * @param indexName
     *            the index name.
     */

    @Override
    protected void dropCatalog(CatalogName indexName, Connection<IStratioStreamingAPI> connection)
                    throws ExecutionException, UnsupportedException {
        throw new UnsupportedException("Drop catalog not supported in Streaming connector");

    }

    /**
     * This method drop a type in Streaming.
     *
     *
     * @param stream
     *            the stream name.
     */
    @Override
    protected void dropTable(TableName stream, Connection<IStratioStreamingAPI> connection) throws ExecutionException,
                    UnsupportedException {
        String streamName = stream.getName();
        try {

            connection.getNativeConnection().dropStream(streamName);
        } catch (StratioStreamingException e) {
            String msg = "Fail droping the Stream [" + streamName + "]. " + e.getMessage();
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
    protected void dropIndex(IndexMetadata indexMetadata, Connection connection) throws UnsupportedException,
                    ExecutionException {
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
