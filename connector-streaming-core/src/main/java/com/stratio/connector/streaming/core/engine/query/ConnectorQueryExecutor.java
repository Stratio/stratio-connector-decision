package com.stratio.connector.streaming.core.engine.query;

import com.stratio.connector.commons.connection.Connection;
import com.stratio.connector.streaming.core.engine.query.util.StreamUtil;
import com.stratio.meta.common.logicalplan.Project;
import com.stratio.streaming.api.IStratioStreamingAPI;
import com.stratio.streaming.api.StratioStreamingAPI;
import com.stratio.streaming.commons.exceptions.StratioAPISecurityException;
import com.stratio.streaming.commons.exceptions.StratioEngineOperationException;
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
            ConnectorQueryData queryData) throws StratioEngineOperationException, StratioAPISecurityException {


            IStratioStreamingAPI stratioStreamingAPI = connection.getNativeConnection();
            String streamOutgoingName = StreamUtil.createOutgoingName(StreamUtil.createStreamName(queryData
                    .getProjection()),"01234"); //TODO

             stratioStreamingAPI.addQuery(streamOutgoingName, query);

            KafkaStream<String, StratioStreamingMessage> streams = stratioStreamingAPI.listenStream(streamOutgoingName);
            for (MessageAndMetadata stream: streams) {
                StratioStreamingMessage theMessage = (StratioStreamingMessage)stream.message();
                for (ColumnNameTypeValue column: theMessage.getColumns()) {
                    System.out.println("Column: "+column.getColumn());
                    System.out.println("Value: "+column.getValue());
                    System.out.println("Type: "+column.getType());
                }
            }


        return "queryID";//TODO
    }
}
