package com.stratio.connector.streaming.core.engine.query.queryExecutor;

import com.stratio.connector.commons.connection.Connection;
import com.stratio.connector.streaming.core.engine.query.ConnectorQueryData;
import com.stratio.connector.streaming.core.engine.query.util.StreamResultSet;
import com.stratio.connector.streaming.core.engine.query.util.StreamUtil;
import com.stratio.meta.common.connector.IResultHandler;
import com.stratio.streaming.api.IStratioStreamingAPI;
import com.stratio.streaming.commons.exceptions.StratioAPISecurityException;
import com.stratio.streaming.commons.exceptions.StratioEngineOperationException;
import com.stratio.streaming.commons.exceptions.StratioEngineStatusException;
import com.stratio.streaming.commons.messages.StratioStreamingMessage;

import kafka.consumer.KafkaStream;
import kafka.message.MessageAndMetadata;

/**
 * Created by jmgomez on 30/09/14.
 */
public abstract class ConnectorQueryExecutor {

    String queryId;
    StreamResultSet streamResultSet;
    ConnectorQueryData queryData;

    public ConnectorQueryExecutor(ConnectorQueryData queryData) {
        this.queryData = queryData;
        streamResultSet = new StreamResultSet(queryData);
    }

    public void executeQuery(String query, Connection<IStratioStreamingAPI> connection, ConnectorQueryData queryData,
                    IResultHandler resultHandler) throws StratioEngineOperationException, StratioAPISecurityException,
                    StratioEngineStatusException, InterruptedException {

        IStratioStreamingAPI stratioStreamingAPI = connection.getNativeConnection();
        String streamName = StreamUtil.createStreamName(queryData.getProjection());
        String streamOutgoingName = StreamUtil.createOutgoingName(streamName, queryData.getQueryId());
        System.out.println("********************** Creating query...");
        System.out.println(query);
        queryId = stratioStreamingAPI.addQuery(streamName, query);

        System.out.println("********************** Listening..." + streamOutgoingName);
        KafkaStream<String, StratioStreamingMessage> streams = stratioStreamingAPI.listenStream(streamOutgoingName);
        System.out.println("********************** Wait for next 	...");
        int i = 0;

        for (MessageAndMetadata stream : streams) {
            // TODO the send the metaInfo
            // TODO how to send the correct window
            StratioStreamingMessage theMessage = (StratioStreamingMessage) stream.message();
            processMessage(theMessage, resultHandler);
        }

    }

    protected abstract void processMessage(StratioStreamingMessage theMessage, IResultHandler resultHandler);

    public void endQuery(String streamName, Connection<IStratioStreamingAPI> connection)
                    throws StratioEngineStatusException, StratioAPISecurityException, StratioEngineOperationException {
        IStratioStreamingAPI streamConection = connection.getNativeConnection();
        streamConection.stopListenStream(streamName);
        if (queryId != null) {
            streamConection.removeQuery(streamName, queryId);
        }

    }

}
