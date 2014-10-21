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

package com.stratio.connector.streaming.core.engine.query.queryexecutor;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stratio.connector.commons.connection.Connection;
import com.stratio.connector.streaming.core.engine.query.ConnectorQueryData;
import com.stratio.connector.streaming.core.engine.query.queryexecutor.messageProcess.ProcessMessage;
import com.stratio.connector.streaming.core.engine.query.util.StreamUtil;
import com.stratio.meta.common.data.Cell;
import com.stratio.meta.common.data.Row;
import com.stratio.meta.common.exceptions.UnsupportedException;
import com.stratio.streaming.api.IStratioStreamingAPI;
import com.stratio.streaming.commons.exceptions.StratioAPISecurityException;
import com.stratio.streaming.commons.exceptions.StratioEngineOperationException;
import com.stratio.streaming.commons.exceptions.StratioEngineStatusException;
import com.stratio.streaming.commons.messages.ColumnNameTypeValue;
import com.stratio.streaming.commons.messages.StratioStreamingMessage;

import kafka.consumer.KafkaStream;
import kafka.message.MessageAndMetadata;

/**
 * Created by jmgomez on 15/10/14.
 */
public class StreamingQuery {

    /**
     * The Log.
     */
    private final Logger logger = LoggerFactory.getLogger(this.getClass());
    private ConnectorQueryData queryData;
    private String queryId;
    private ProcessMessage processMessage;

    public StreamingQuery(ConnectorQueryData queryData, ProcessMessage processMessage) {
        this.processMessage = processMessage;
        this.queryData = queryData;

    }

    public String createQuery(String query, IStratioStreamingAPI stratioStreamingAPI)
            throws StratioEngineOperationException, StratioEngineStatusException, StratioAPISecurityException,
            UnsupportedException {

        String streamName = StreamUtil.createStreamName(queryData.getProjection());
        String streamOutgoingName = StreamUtil.createOutgoingName(streamName, queryData.getQueryId());
        logger.info("add query...");
        logger.debug(query);
        queryId = stratioStreamingAPI.addQuery(streamName, query);

        return streamOutgoingName;
    }

    public KafkaStream<String, StratioStreamingMessage> listenQuery(IStratioStreamingAPI stratioStreamingAPI,
            String streamOutgoingName)
            throws StratioEngineOperationException, StratioEngineStatusException, StratioAPISecurityException,
            UnsupportedException {
        logger.info("Listening stream..." + streamOutgoingName);
        KafkaStream<String, StratioStreamingMessage> messageAndMetadatas = stratioStreamingAPI
                .listenStream(streamOutgoingName);
        StreamUtil.insertRandomData(stratioStreamingAPI, streamOutgoingName, queryData.getSelect());
        return messageAndMetadatas;
    }

    public void readMessages(KafkaStream<String, StratioStreamingMessage> streams) throws UnsupportedException {
        logger.info("Waiting a message...");
        for (MessageAndMetadata stream : streams) {
            StratioStreamingMessage theMessage = (StratioStreamingMessage) stream.message();

            processMessage.processMessage(getRow(theMessage.getColumns()));

        }

    }

    public void endQuery(String streamName, Connection<IStratioStreamingAPI> connection)
            throws StratioEngineStatusException, StratioAPISecurityException, StratioEngineOperationException {
        IStratioStreamingAPI streamConection = connection.getNativeConnection();
        streamConection.stopListenStream(streamName);
        if (queryId != null) {
            streamConection.removeQuery(streamName, queryId);
        }
        if (processMessage != null) {
            processMessage.end();
        }

    }

    private Row getRow(List<ColumnNameTypeValue> columns) {

        Row row = new Row();
        for (ColumnNameTypeValue column : columns) {
            row.addCell(column.getColumn(), new Cell(column.getValue()));
        }
        return row;

    }

}
