package com.stratio.connector.streaming.core.engine.query;

import com.stratio.connector.commons.connection.Connection;
import com.stratio.connector.streaming.core.engine.query.util.StreamUtil;
import com.stratio.meta.common.connector.IResultHandler;
import com.stratio.meta.common.data.Cell;
import com.stratio.meta.common.data.ResultSet;
import com.stratio.meta.common.data.Row;
import com.stratio.meta.common.result.QueryResult;
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

    String queryId;

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
            System.out.println("key: " + stream.key());
            System.out.println("productarity: " + stream.productArity());

            // TODO the send the metaInfo
            // TODO how to send the correct window
            StratioStreamingMessage theMessage = (StratioStreamingMessage) stream.message();
            ResultSet resultSet = new ResultSet();
            System.out.println("TimeStamp: " + theMessage.getTimestamp());
            System.out.println("getRequest_id: " + theMessage.getRequest_id());
            System.out.println("getSession_id: " + theMessage.getSession_id());
            for (ColumnNameTypeValue column : theMessage.getColumns()) {

                System.out.print(" Column: " + column.getColumn());
                System.out.print(" || Type: " + column.getType());
                System.out.print(" || Value: " + column.getValue());
                System.out.println("\n--------- (" + i + ") -----------------");

                i++;
                resultSet.add(new Row(column.getColumn(), new Cell(column.getValue())));

            }

            // TODO remove duplicates
            QueryResult queryResult = QueryResult.createQueryResult(resultSet);
            resultHandler.processResult(queryResult);
        }

    }

    public void endQuery(String streamName, Connection<IStratioStreamingAPI> connection)
                    throws StratioEngineStatusException, StratioAPISecurityException, StratioEngineOperationException {
        IStratioStreamingAPI streamConection = connection.getNativeConnection();
        streamConection.stopListenStream(streamName);
        if (queryId != null) {
            streamConection.removeQuery(streamName, queryId);
        }

    }

}
