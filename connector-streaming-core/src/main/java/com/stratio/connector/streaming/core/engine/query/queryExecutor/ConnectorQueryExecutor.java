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

package com.stratio.connector.streaming.core.engine.query.queryExecutor;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stratio.connector.commons.connection.Connection;
import com.stratio.connector.streaming.core.engine.query.ConnectorQueryData;
import com.stratio.connector.streaming.core.engine.query.queryExecutor.messageProcess.ProccesMessageFactory;
import com.stratio.connector.streaming.core.engine.query.queryExecutor.messageProcess.ProcessMessage;
import com.stratio.connector.streaming.core.engine.query.util.ResultsetCreator;
import com.stratio.meta.common.connector.IResultHandler;
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
 * Created by jmgomez on 30/09/14.
 */
public class ConnectorQueryExecutor {

    /**
     * The Log.
     */
    final Logger logger = LoggerFactory.getLogger(this.getClass());

    protected String queryId;
    protected ConnectorQueryData queryData;
    IResultHandler resultHandler;

    List<Integer> rowOrder;
    ProcessMessage proccesMesage;
    StreamingQuery streamingQuery;

    public ConnectorQueryExecutor(ConnectorQueryData queryData, IResultHandler resultHandler)
            throws UnsupportedException {
        this.queryData = queryData;
        this.resultHandler = resultHandler;
        rowOrder = new ArrayList<Integer>();


    }

    public void executeQuery(String query, Connection<IStratioStreamingAPI> connection)
            throws StratioEngineOperationException, StratioAPISecurityException, StratioEngineStatusException,
            InterruptedException, UnsupportedException {


        IStratioStreamingAPI stratioStreamingAPI = connection.getNativeConnection();
        ResultsetCreator resultSetCreator = new ResultsetCreator(queryData);
        resultSetCreator.setResultHandler(resultHandler);
        proccesMesage = ProccesMessageFactory.getProccesMessage(queryData, resultSetCreator);

        streamingQuery = new StreamingQuery(queryData, proccesMesage);
        String streamOutgoingName = streamingQuery.createQuery(query, stratioStreamingAPI);

        KafkaStream<String, StratioStreamingMessage> stream = streamingQuery.listenQuery(stratioStreamingAPI,
                streamOutgoingName);

        //readMessages(stream);

        streamingQuery.readMessages(stream);

    }

    private void readMessages(KafkaStream<String, StratioStreamingMessage> streams) throws UnsupportedException {
        logger.info("Waiting a message...");
        ResultsetCreator resultsetCreator = new ResultsetCreator(queryData);
        proccesMesage = ProccesMessageFactory.getProccesMessage(queryData, resultsetCreator);
        for (MessageAndMetadata stream : streams) {
            StratioStreamingMessage theMessage = (StratioStreamingMessage) stream.message();

            proccesMesage.processMessage(getSortRow(theMessage.getColumns()));

        }
    }



    public void endQuery(String streamName, Connection<IStratioStreamingAPI> connection)
            throws StratioEngineStatusException, StratioAPISecurityException, StratioEngineOperationException {
        streamingQuery.endQuery(streamName,connection);

    }



    protected Row getSortRow(List<ColumnNameTypeValue> columns) {

        Row row = new Row();
        for (ColumnNameTypeValue column : columns) {
            row.addCell(column.getColumn(), new Cell(column.getValue()));
        }
        return row;

    }

}
