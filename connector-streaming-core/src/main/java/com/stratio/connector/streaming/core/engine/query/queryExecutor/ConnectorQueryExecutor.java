package com.stratio.connector.streaming.core.engine.query.queryExecutor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stratio.connector.commons.connection.Connection;
import com.stratio.connector.streaming.core.engine.query.ConnectorQueryData;
import com.stratio.connector.streaming.core.engine.query.util.StreamResultSet;
import com.stratio.connector.streaming.core.engine.query.util.StreamUtil;
import com.stratio.meta.common.connector.IResultHandler;
import com.stratio.meta.common.exceptions.UnsupportedException;
import com.stratio.meta.common.logicalplan.Select;
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


    /**
     * The Log.
     */
    final Logger logger = LoggerFactory.getLogger(this.getClass());

    protected String queryId;
    protected ConnectorQueryData queryData;
    IResultHandler resultHandler;

    public ConnectorQueryExecutor(ConnectorQueryData queryData,IResultHandler resultHandler) {
        this.queryData = queryData;
        this.resultHandler = resultHandler;

    }

    public void executeQuery(String query, Connection<IStratioStreamingAPI> connection )
            throws StratioEngineOperationException, StratioAPISecurityException,
            StratioEngineStatusException, InterruptedException, UnsupportedException {

        IStratioStreamingAPI stratioStreamingAPI = connection.getNativeConnection();
        String streamName = StreamUtil.createStreamName(queryData.getProjection());
        String streamOutgoingName = StreamUtil.createOutgoingName(streamName, queryData.getQueryId());
        logger.info("add query...");
        logger.debug(query);
        queryId = stratioStreamingAPI.addQuery(streamName, query);

        logger.info("Listening stream..." + streamOutgoingName);
        KafkaStream<String, StratioStreamingMessage> streams = stratioStreamingAPI.listenStream(streamOutgoingName);

        StreamUtil.insertRandomData(stratioStreamingAPI, streamOutgoingName,queryData.getSelect());
        logger.info("Waiting a message...");
       for (MessageAndMetadata stream : streams) {
            // TODO the send the metaInfo

            StratioStreamingMessage theMessage = (StratioStreamingMessage) stream.message();

            processMessage(theMessage);
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

    protected abstract void processMessage(StratioStreamingMessage theMessage);


}
