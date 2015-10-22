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
package com.stratio.connector.decision.core.engine;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import com.stratio.connector.commons.TimerJ;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stratio.connector.commons.connection.Connection;
import com.stratio.connector.commons.connection.ConnectionHandler;
import com.stratio.connector.commons.engine.CommonsStorageEngine;
import com.stratio.connector.decision.core.engine.query.util.StreamUtil;
import com.stratio.crossdata.common.data.Cell;
import com.stratio.crossdata.common.data.Row;
import com.stratio.crossdata.common.data.TableName;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.exceptions.UnsupportedException;
import com.stratio.crossdata.common.logicalplan.Filter;
import com.stratio.crossdata.common.metadata.TableMetadata;
import com.stratio.crossdata.common.statements.structures.Relation;
import com.stratio.decision.api.IStratioStreamingAPI;
import com.stratio.decision.api.messaging.ColumnNameValue;
import com.stratio.decision.commons.exceptions.StratioStreamingException;

/**
 * This class performs operations insert and delete in Decision.
 */

public class DecisionStorageEngine extends CommonsStorageEngine<IStratioStreamingAPI> {

    /**
     * The Log.
     */
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    /**
     * Constructor.
     *
     * @param connectionHandler
     *            the connection handler.
     */
    public DecisionStorageEngine(ConnectionHandler connectionHandler) {

        super(connectionHandler);

    }

    /**
     * Insert a document in Decision.
     *
     * @param targetStream
     *            the targetName.
     * @param row
     *            the row.
     * @throws ExecutionException
     *             in case of failure during the execution.
     *
     *  @throws UnsupportedException if the operation is not supported.
     */

    @Override
    @TimerJ
    protected void insert(TableMetadata targetStream, Row row, boolean isNotExists, Connection<IStratioStreamingAPI>
            connection)
            throws ExecutionException, UnsupportedException {
        if (isNotExists){
            throw new UnsupportedException("if not exist is not supporting in Decision connector");
        }
        String streamName = StreamUtil.createStreamName(targetStream.getName());
        try {
            List<ColumnNameValue> streamData = new ArrayList<ColumnNameValue>();
            Map<String, Cell> cells = row.getCells();
            for (Map.Entry<String, Cell> cellName : cells.entrySet()) {

                streamData.add(new ColumnNameValue(cellName.getKey(), cellName.getValue().getValue()));
            }
            connection.getNativeConnection().insertData(streamName, streamData);
        } catch (StratioStreamingException e) {
            String msg = "Inserting data error in Decision [" + streamName + "]. " + e.getMessage();
            logger.error(msg);
            throw new ExecutionException(msg, e);
        }
    }

    /**
     * Insert a set of documents in Decision.
     *
     * @param rows
     *            the set of rows.
     * @throws ExecutionException
     *             in case of failure during the execution.
     *  @throws UnsupportedException if the operation is not supported.
     */
    @Override
    @TimerJ
    protected void insert(TableMetadata targetStream, Collection<Row> rows, boolean isNotExists,
            Connection<IStratioStreamingAPI> connection)
            throws ExecutionException, UnsupportedException {
        if (isNotExists){
            throw new UnsupportedException("if not exist is not supporting in Decision connector");
        }
        String streamName = targetStream.getName().getName();

        try {
            for (Row row : rows) {
                insert(targetStream, row, isNotExists,connection);
            }
        } catch (ExecutionException e) {
            String msg = "Inserting bulk data error in Decision. [" + streamName + "]. " + e.getMessage();
            logger.error(msg);
            throw new ExecutionException(msg, e);
        }
    }

    @Override
    protected void truncate(TableName tableName, Connection<IStratioStreamingAPI> connection)
                    throws UnsupportedException, ExecutionException {
        throw new UnsupportedException("Truncate is not supported");

    }

    @Override
    protected void delete(TableName tableName, Collection<Filter> whereClauses,
                    Connection<IStratioStreamingAPI> connection) throws UnsupportedException, ExecutionException {
        throw new UnsupportedException("Delete is not supported");

    }

    @Override
    protected void update(TableName tableName, Collection<Relation> assignments, Collection<Filter> whereClauses,
                    Connection<IStratioStreamingAPI> connection) throws UnsupportedException, ExecutionException {
        throw new UnsupportedException("Update is not supported");

    }


}
