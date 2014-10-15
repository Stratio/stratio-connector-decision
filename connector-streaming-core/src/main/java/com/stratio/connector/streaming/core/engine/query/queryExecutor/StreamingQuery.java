package com.stratio.connector.streaming.core.engine.query.queryExecutor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stratio.connector.streaming.core.engine.query.ConnectorQueryData;
import com.stratio.connector.streaming.core.engine.query.util.StreamUtil;
import com.stratio.meta.common.exceptions.UnsupportedException;
import com.stratio.streaming.api.IStratioStreamingAPI;
import com.stratio.streaming.commons.exceptions.StratioAPISecurityException;
import com.stratio.streaming.commons.exceptions.StratioEngineOperationException;
import com.stratio.streaming.commons.exceptions.StratioEngineStatusException;
import com.stratio.streaming.commons.messages.StratioStreamingMessage;

import kafka.consumer.KafkaStream;

/**
 * Created by jmgomez on 15/10/14.
 */
public class StreamingQuery {

    public ConnectorQueryData queryData;

    public StreamingQuery(ConnectorQueryData queryData){
        this.queryData = queryData;

    }

    /**
     * The Log.
     */
    final Logger logger = LoggerFactory.getLogger(this.getClass());


    private String queryId;

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


    public KafkaStream<String, StratioStreamingMessage> listenQurey(IStratioStreamingAPI stratioStreamingAPI,
            String streamOutgoingName) throws  StratioEngineOperationException, StratioEngineStatusException, StratioAPISecurityException,
            UnsupportedException{
        logger.info("Listening stream..." + streamOutgoingName);
        KafkaStream<String, StratioStreamingMessage> messageAndMetadatas = stratioStreamingAPI
                .listenStream(streamOutgoingName);
        StreamUtil.insertRandomData(stratioStreamingAPI, streamOutgoingName, queryData.getSelect());
        return messageAndMetadatas;
    }




}
