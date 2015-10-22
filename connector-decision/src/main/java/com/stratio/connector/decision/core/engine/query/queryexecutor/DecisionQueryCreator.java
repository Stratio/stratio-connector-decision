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

package com.stratio.connector.decision.core.engine.query.queryexecutor;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stratio.connector.commons.connection.Connection;
import com.stratio.connector.commons.util.ColumnTypeHelper;
import com.stratio.connector.decision.core.engine.query.ConnectorQueryData;
import com.stratio.connector.decision.core.engine.query.queryexecutor.messageprocess.ProcessMessage;
import com.stratio.connector.decision.core.engine.query.util.StreamUtil;
import com.stratio.connector.decision.core.exception.ExecutionValidationException;
import com.stratio.crossdata.common.data.Cell;
import com.stratio.crossdata.common.data.Row;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.decision.api.IStratioStreamingAPI;
import com.stratio.decision.commons.exceptions.StratioAPISecurityException;
import com.stratio.decision.commons.exceptions.StratioEngineOperationException;
import com.stratio.decision.commons.exceptions.StratioEngineStatusException;
import com.stratio.decision.commons.messages.ColumnNameTypeValue;
import com.stratio.decision.commons.messages.StratioStreamingMessage;

import kafka.consumer.KafkaStream;
import kafka.message.MessageAndMetadata;

/**
 * This class represents a query creator.
 */
public class DecisionQueryCreator {

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
     *
     * @param queryData
     *            the query data.
     * @param processMessage
     *            the message processor.
     */
    public DecisionQueryCreator(ConnectorQueryData queryData, ProcessMessage processMessage) {
        this.processMessage = processMessage;
        this.queryData = queryData;

    }

    /**
     * This method send a query to decision.
     *
     * @param query
     *            the query to send.
     * @param stratioDecisionAPI
     *            the decision api.
     * @return the query id.
     * @throws ExecutionException
     *             if the execution fail.
     * @throws ExecutionValidationException
     *             if a operation is not supported.
     */
    public String createQuery(String query, IStratioStreamingAPI stratioDecisionAPI)
                    throws ExecutionValidationException, ExecutionException {
        String streamOutgoingName = "";
        try {
            String streamName = StreamUtil.createStreamName(queryData.getProjection());
            streamOutgoingName = StreamUtil.createOutgoingName(streamName, queryData.getQueryId());
            logger.info("add query...");
            logger.debug(query);
            queryId = stratioDecisionAPI.addQuery(streamName, query);
        } catch (StratioEngineOperationException | StratioEngineStatusException | StratioAPISecurityException e) {
            String msg = "Decision query creation fail." + e.getMessage();
            logger.error(msg);
            throw new ExecutionException(msg, e);
        }

        return streamOutgoingName;
    }

    /**
     * This method listen a streami query.
     *
     * @param stratioDecisionAPI
     *            the stratio straming api.
     * @param streamOutgoingName
     *            the query name.
     * @return the query result.
     * @throws ExecutionValidationException
     *             if an operation is not supported.
     * @throws ExecutionException
     *             if a error happen.
     */
    public KafkaStream<String, StratioStreamingMessage> listenQuery(IStratioStreamingAPI stratioDecisionAPI,
                    String streamOutgoingName) throws ExecutionValidationException, ExecutionException {
        KafkaStream<String, StratioStreamingMessage> messageAndMetadatas = null;
        try {
            logger.info("Listening stream..." + streamOutgoingName);

            messageAndMetadatas = stratioDecisionAPI.listenStream(streamOutgoingName);

            StreamUtil.insertRandomData(stratioDecisionAPI, streamOutgoingName, queryData.getSelect());
        } catch (StratioAPISecurityException | StratioEngineStatusException e) {
            String msg = "Decision listen query creation fail." + e.getMessage();
            logger.error(msg);
            throw new ExecutionException(msg, e);
        }

        return messageAndMetadatas;
    }

    /**
     * This method read a message.
     *
     * @param streams
     * @throws ExecutionValidationException
     * @throws ExecutionException
     */
    public void readMessages(KafkaStream<String, StratioStreamingMessage> streams) throws ExecutionValidationException,
                    ExecutionException {
        logger.info("Waiting a message...");

        for (MessageAndMetadata stream : streams) {
            StratioStreamingMessage theMessage = (StratioStreamingMessage) stream.message();

            processMessage.processMessage(getRow(theMessage.getColumns()));

        }

    }

    /**
     * This method finish the decision query.
     *
     * @param streamName
     *            the stream name.
     * @param connection
     *            the connection.
     * @throws ExecutionException
     *             if a fail happens.
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
        } catch (StratioAPISecurityException | StratioEngineOperationException | StratioEngineStatusException e) {
            String msg = "Decision end query creation fail." + e.getMessage();
            logger.error(msg);
            throw new ExecutionException(msg, e);
        }

    }

    /**
     * create a row.
     *
     * @param columns
     *            the columns values.
     * @return the row.
     * @throws ExecutionException
     *             if any error happens.
     */
    private Row getRow(List<ColumnNameTypeValue> columns) throws ExecutionException {

        Row row = new Row();
        for (ColumnNameTypeValue column : columns) {

            logger.warn("native: " + column.getColumn() + " type--> " + column.getType() + " value -> "
                            + column.getValue());

            Object value = ColumnTypeHelper.getCastingValue(queryData.getSelect().getTypeMap().get(column.getColumn()),
                            column.getValue());
            row.addCell(column.getColumn(), new Cell(value));
        }
        return row;

    }

}
