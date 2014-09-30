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
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stratio.connector.commons.connection.Connection;
import com.stratio.connector.commons.connection.ConnectionHandler;
import com.stratio.connector.commons.engine.CommonsStorageEngine;
import com.stratio.meta.common.data.Cell;
import com.stratio.meta.common.data.Row;
import com.stratio.meta.common.exceptions.ExecutionException;
import com.stratio.meta.common.exceptions.UnsupportedException;
import com.stratio.meta2.common.metadata.TableMetadata;
import com.stratio.streaming.api.IStratioStreamingAPI;
import com.stratio.streaming.api.messaging.ColumnNameValue;
import com.stratio.streaming.commons.exceptions.StratioStreamingException;

/**
 * This class performs operations insert and delete in Streaming.
 */

public class StreamingStorageEngine extends CommonsStorageEngine<IStratioStreamingAPI> {

    /**
     * The Log.
     */
    final Logger logger = LoggerFactory.getLogger(this.getClass());

    /**
     * Constructor.
     *
     * @param connectionHandler
     *            the connection handler.
     */
    public StreamingStorageEngine(ConnectionHandler connectionHandler) {

        super(connectionHandler);
    }

    /**
     * Insert a document in Streaming.
     *
     *
     * @param targetStream
     *            the targetName.
     * @param row
     *            the row.
     * @throws com.stratio.meta.common.exceptions.ExecutionException
     *             in case of failure during the execution.
     * @throws com.stratio.meta.common.exceptions.UnsupportedException
     *             it the operation is not supported.
     */

    @Override
    protected void insert(TableMetadata targetStream, Row row, Connection<IStratioStreamingAPI> connection)
                    throws UnsupportedException, ExecutionException {
        String streamName = targetStream.getName().getName();
        try {
            List<ColumnNameValue> streamData = new ArrayList<ColumnNameValue>();
            Map<String, Cell> cells = row.getCells();
            for (String cellName : cells.keySet()) {

                streamData.add(new ColumnNameValue(cellName, cells.get(cellName).getValue()));
            }
            connection.getNativeConnection().insertData(streamName, streamData);
        } catch (StratioStreamingException e) {
            String msg = "Inserting data error in Streaming [" + streamName + "]. " + e.getMessage();
            logger.error(msg);
            throw new ExecutionException(msg, e);
        }
    }

    /**
     * Insert a set of documents in Streaming.
     *
     *
     * @param rows
     *            the set of rows.
     * @throws com.stratio.meta.common.exceptions.ExecutionException
     *             in case of failure during the execution.
     * @throws com.stratio.meta.common.exceptions.UnsupportedException
     *             if the operation is not supported.
     */
    @Override
    protected void insert(TableMetadata targetStream, Collection<Row> rows, Connection<IStratioStreamingAPI> connection)
                    throws UnsupportedException, ExecutionException {
        String streamName = targetStream.getName().getName();
        try {
            for (Row row : rows) {
                insert(targetStream, row, connection);
            }
        } catch (ExecutionException e) {
            String msg = "Inserting bulk data error in Streaming. [" + streamName + "]. " + e.getMessage();
            logger.error(msg);
            throw new ExecutionException(msg, e);
        }
    }

}
