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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stratio.connector.commons.connection.Connection;
import com.stratio.connector.commons.connection.ConnectionHandler;
import com.stratio.connector.commons.engine.CommonsStorageEngine;
import com.stratio.connector.streaming.core.engine.query.util.StreamUtil;
import com.stratio.crossdata.common.data.Cell;
import com.stratio.crossdata.common.data.Row;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.exceptions.UnsupportedException;
import com.stratio.crossdata.common.metadata.TableMetadata;
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
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    /**
     * Constructor.
     *
     * @param connectionHandler the connection handler.
     */
    public StreamingStorageEngine(ConnectionHandler connectionHandler) {

        super(connectionHandler);
    }

    /**
     * Insert a document in Streaming.
     *
     * @param targetStream the targetName.
     * @param row          the row.
     * @throws ExecutionException   in case of failure during the execution.
     * @throws UnsupportedException it the operation is not supported.
     */

    @Override
    protected void insert(TableMetadata targetStream, Row row, Connection<IStratioStreamingAPI> connection)
            throws UnsupportedException, ExecutionException {
        String streamName = StreamUtil.createStreamName(targetStream.getName());
        try {
            List<ColumnNameValue> streamData = new ArrayList<ColumnNameValue>();
            Map<String, Cell> cells = row.getCells();
            for (Map.Entry<String, Cell> cellName : cells.entrySet()) {

                streamData.add(new ColumnNameValue(cellName.getKey(), cellName.getValue().getValue()));
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
     * @param rows the set of rows.
     * @throws ExecutionException   in case of failure during the execution.
     * @throws UnsupportedException if the operation is not supported.
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
