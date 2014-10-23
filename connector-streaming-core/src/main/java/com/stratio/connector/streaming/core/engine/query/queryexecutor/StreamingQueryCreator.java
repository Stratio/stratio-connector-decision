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

package com.stratio.connector.streaming.core.engine.query.queryexecutor;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stratio.connector.commons.connection.Connection;
import com.stratio.connector.streaming.core.engine.query.ConnectorQueryData;
import com.stratio.connector.streaming.core.engine.query.queryexecutor.messageprocess.ProcessMessage;
import com.stratio.connector.streaming.core.engine.query.util.StreamUtil;
import com.stratio.crossdata.common.data.Cell;
import com.stratio.crossdata.common.data.Row;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.exceptions.UnsupportedException;
import com.stratio.streaming.api.IStratioStreamingAPI;
import com.stratio.streaming.commons.exceptions.StratioAPISecurityException;
import com.stratio.streaming.commons.exceptions.StratioEngineOperationException;
import com.stratio.streaming.commons.exceptions.StratioEngineStatusException;
import com.stratio.streaming.commons.messages.ColumnNameTypeValue;
import com.stratio.streaming.commons.messages.StratioStreamingMessage;

import kafka.consumer.KafkaStream;
import kafka.message.MessageAndMetadata;

/**
 * This class represents a query creator.
 * Created by jmgomez on 15/10/14.
 */
public class StreamingQueryCreator {

    /**
     * The Log.
     */
    private final Logger logger = LoggerFactory.getLogger(this.getClass());
    /**
     * The query data.
     */
    private ConnectorQueryData queryData;
    /**
     * The queryId.
     */
    private String queryId;
    /**
     * A Message processor.
     */
    private ProcessMessage processMessage;

    /**
     * Constructor.
     * @param queryData the query data.
     * @param processMessage the message processor.
     */
    public StreamingQueryCreator(ConnectorQueryData queryData, ProcessMessage processMessage) {
        this.processMessage = processMessage;
        this.queryData = queryData;

    }

    /**
     * This method send a  query in streaming.
     * @param query the query to send.
     * @param stratioStreamingAPI the streaming api.
     * @return the query id.
     * @throws ExecutionException if the execution fail.
     * @throws UnsupportedException if a operation is not supported.
     */
    public String createQuery(String query, IStratioStreamingAPI stratioStreamingAPI)
            throws UnsupportedException, ExecutionException {
        String streamOutgoingName = "";
        try {
            String streamName = StreamUtil.createStreamName(queryData.getProjection());
            streamOutgoingName = StreamUtil.createOutgoingName(streamName, queryData.getQueryId());
            logger.info("add query...");
            logger.debug(query);
            queryId = stratioStreamingAPI.addQuery(streamName, query);
        }catch( StratioEngineOperationException | StratioEngineStatusException  | StratioAPISecurityException e)   {
            String msg = "Streaming query creation fail." + e.getMessage();
            logger.error(msg);
            throw new ExecutionException(msg, e);
        }

        return streamOutgoingName;
    }

    /**
     * This method listen a streami query.
     * @param stratioStreamingAPI the stratio straming api.
     * @param streamOutgoingName the query name.
     * @return the query result.
     * @throws UnsupportedException if an operation is not supported.
     * @throws ExecutionException  if a error happen.
     */
    public KafkaStream<String, StratioStreamingMessage> listenQuery(IStratioStreamingAPI stratioStreamingAPI,
            String streamOutgoingName) throws UnsupportedException, ExecutionException {
        KafkaStream<String, StratioStreamingMessage> messageAndMetadatas = null;
        try {
            logger.info("Listening stream..." + streamOutgoingName);
             messageAndMetadatas = stratioStreamingAPI
                    .listenStream(streamOutgoingName);
            StreamUtil.insertRandomData(stratioStreamingAPI, streamOutgoingName, queryData.getSelect());
        }catch( StratioAPISecurityException | StratioEngineStatusException e)   {
                String msg = "Streaming listen query creation fail." + e.getMessage();
                logger.error(msg);
                throw new ExecutionException(msg, e);
            }


        return messageAndMetadatas;
    }

    /**
     * This metod read a message.
     * @param streams
     * @throws UnsupportedException
     */
    public void readMessages(KafkaStream<String, StratioStreamingMessage> streams) throws UnsupportedException {
        logger.info("Waiting a message...");
        for (MessageAndMetadata stream : streams) {
            StratioStreamingMessage theMessage = (StratioStreamingMessage) stream.message();

            processMessage.processMessage(getRow(theMessage.getColumns()));

        }

    }

    /**
     * This method finish the streaming query.
     * @param streamName the stream name.
     * @param connection the connection.
     * @throws ExecutionException if a fail happens.
     */
    public void endQuery(String streamName, Connection<IStratioStreamingAPI> connection) throws ExecutionException {
        try {

            IStratioStreamingAPI streamConection = connection.getNativeConnection();
            streamConection.stopListenStream(streamName);
            if (queryId != null) {
                streamConection.removeQuery(streamName, queryId);
            }
            if (processMessage != null) {
                processMessage.end();
            }
        }catch( StratioAPISecurityException | StratioEngineOperationException  | StratioEngineStatusException e){
            String msg = "Streaming end query creation fail." + e.getMessage();
            logger.error(msg);
            throw new ExecutionException(msg, e);
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
