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



import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stratio.connector.commons.connection.Connection;
import com.stratio.connector.streaming.core.engine.query.ConnectorQueryData;
import com.stratio.connector.streaming.core.engine.query.queryexecutor.messageprocess.ProcessMessage;
import com.stratio.connector.streaming.core.engine.query.queryexecutor.messageprocess.ProcessMessageFactory;
import com.stratio.connector.streaming.core.engine.query.util.ResultsetCreator;
import com.stratio.crossdata.common.connector.IResultHandler;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.exceptions.UnsupportedException;
import com.stratio.streaming.api.IStratioStreamingAPI;
import com.stratio.streaming.commons.messages.StratioStreamingMessage;

import kafka.consumer.KafkaStream;

/**
 * This class responsibility is to control the process to execute a query.
 * Created by jmgomez on 30/09/14.
 */
public class ConnectorQueryExecutor {


    /**
     * The Log.
     */
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    /**
     * The queryData.
     */
    protected ConnectorQueryData queryData;
    /**
     * The result handler.
     */
    private IResultHandler resultHandler;

    /**
     * The message processor.
     */
    private ProcessMessage proccesMesage;
    /**
     * The streaming queryCreator.
     */
    private StreamingQueryCreator streamingQueryCreator;

    /**
     * Constructor
     * @param queryData the query data.
     * @param resultHandler the result handler.
     * @throws UnsupportedException if an operation is not supported.
     */
    public ConnectorQueryExecutor(ConnectorQueryData queryData, IResultHandler resultHandler)
            throws UnsupportedException {
        this.queryData = queryData;
        this.resultHandler = resultHandler;

    }

    /**
     * This method execute a query.
     * @param query the query.
     * @param connection the connection.
     * @throws ExecutionException if fail the execution.
     * @throws InterruptedException when the asynchronous query stop.
     * @throws UnsupportedException if a operation is not supported.
     */
    public void executeQuery(String query, Connection<IStratioStreamingAPI> connection)
            throws InterruptedException, UnsupportedException, ExecutionException {

        IStratioStreamingAPI stratioStreamingAPI = connection.getNativeConnection();
        ResultsetCreator resultSetCreator = new ResultsetCreator(queryData);
        resultSetCreator.setResultHandler(resultHandler);
        proccesMesage = ProcessMessageFactory.getProccesMessage(queryData, resultSetCreator);

        streamingQueryCreator = new StreamingQueryCreator(queryData, proccesMesage);
        String streamOutgoingName = streamingQueryCreator.createQuery(query, stratioStreamingAPI);

        KafkaStream<String, StratioStreamingMessage> stream = streamingQueryCreator.listenQuery(stratioStreamingAPI,
                streamOutgoingName);

        streamingQueryCreator.readMessages(stream);


    }

    /**
     * This method finalize the query execution.
     * @param streamName the stream name.
     * @param connection the connection.
     * @throws ExecutionException if fail the operation.
     */
    public void endQuery(String streamName, Connection<IStratioStreamingAPI> connection) throws ExecutionException {

             streamingQueryCreator.endQuery(streamName, connection);

    }

}
