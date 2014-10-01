package com.stratio.connector.streaming.core.engine.query;

import com.stratio.connector.commons.connection.Connection;
import com.stratio.connector.streaming.core.engine.query.util.StreamUtil;
import com.stratio.meta.common.logicalplan.Project;
import com.stratio.streaming.api.IStratioStreamingAPI;
import com.stratio.streaming.api.StratioStreamingAPI;
import com.stratio.streaming.commons.exceptions.StratioAPISecurityException;
import com.stratio.streaming.commons.exceptions.StratioEngineOperationException;
import com.stratio.streaming.commons.exceptions.StratioEngineStatusException;
import com.stratio.streaming.commons.exceptions.StratioStreamingException;
import com.stratio.streaming.commons.messages.ColumnNameTypeValue;
import com.stratio.streaming.commons.messages.StratioStreamingMessage;

import kafka.consumer.KafkaStream;
import kafka.message.MessageAndMetadata;

/**
 * Created by jmgomez on 30/09/14.
 */
public class ConnectorQueryExecutor {
    public String executeQuery(String query, Connection<IStratioStreamingAPI> connection,
            ConnectorQueryData queryData) throws StratioEngineOperationException, StratioAPISecurityException,
            StratioEngineStatusException {


            IStratioStreamingAPI stratioStreamingAPI = connection.getNativeConnection();
        String streamName = StreamUtil.createStreamName(queryData
                .getProjection());
        String streamOutgoingName = StreamUtil.createOutgoingName(streamName,"01234"); //TODO

             stratioStreamingAPI.addQuery(streamName, query);

            KafkaStream<String, StratioStreamingMessage> streams = stratioStreamingAPI.listenStream(streamOutgoingName);
            for (MessageAndMetadata stream: streams) {
                StratioStreamingMessage theMessage = (StratioStreamingMessage)stream.message();
                for (ColumnNameTypeValue column: theMessage.getColumns()) {
                    System.out.print(" Column: "+column.getColumn());
                    System.out.print(" Value: "+column.getValue());
                    System.out.print(" Type: "+column.getType());
                }
                System.out.println("");
            }


        return "queryID";//TODO
    }
}
