package com.stratio.connector.streaming.core.engine.query.queryExecutor;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stratio.connector.commons.connection.Connection;
import com.stratio.connector.streaming.core.engine.query.ConnectorQueryData;
import com.stratio.connector.streaming.core.engine.query.util.StreamUtil;
import com.stratio.meta.common.connector.IResultHandler;
import com.stratio.meta.common.exceptions.UnsupportedException;
import com.stratio.meta.common.logicalplan.Project;
import com.stratio.meta.common.logicalplan.Select;
import com.stratio.meta.common.metadata.structures.ColumnMetadata;
import com.stratio.meta2.common.data.ColumnName;
import com.stratio.meta2.common.metadata.ColumnType;
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
    List<ColumnMetadata> columnsMetadata = new ArrayList<ColumnMetadata>();

    public ConnectorQueryExecutor(ConnectorQueryData queryData, IResultHandler resultHandler) {
        this.queryData = queryData;
        this.resultHandler = resultHandler;
        setColumnMetadata();

    }

    public void executeQuery(String query, Connection<IStratioStreamingAPI> connection)
                    throws StratioEngineOperationException, StratioAPISecurityException, StratioEngineStatusException,
                    InterruptedException, UnsupportedException {

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

    /**
     * 
     */
    private void setColumnMetadata() {
        List<ColumnMetadata> columnsMetadata = new ArrayList<>();
        Select select = queryData.getSelect();
        Project projection = queryData.getProjection();

        for (ColumnName colName : select.getColumnMap().keySet()) {
            String field = colName.getName();
            ColumnType colType = select.getTypeMap().get(colName.getQualifiedName());
            colType = updateColumnType(colType);
            ColumnMetadata columnMetadata = new ColumnMetadata(projection.getTableName().getName(), field, colType);
            columnMetadata.setColumnAlias(select.getColumnMap().get(colName));
            columnsMetadata.add(columnMetadata);
        }

    }

    /**
     * @param colType
     * @return
     */
    private ColumnType updateColumnType(ColumnType colType) {
        // switch (colType) {
        //
        // case BIGINT:
        // returnType = com.stratio.streaming.commons.constants.ColumnType.LONG;
        // break;
        // case BOOLEAN:
        // returnType = com.stratio.streaming.commons.constants.ColumnType.BOOLEAN;
        // break;
        // case DOUBLE:
        // returnType = com.stratio.streaming.commons.constants.ColumnType.DOUBLE;
        // break;
        // case FLOAT:
        // returnType = com.stratio.streaming.commons.constants.ColumnType.FLOAT;
        // break;
        // case INT:
        // returnType = com.stratio.streaming.commons.constants.ColumnType.INTEGER;
        // break;
        // case TEXT:
        // case VARCHAR:
        // returnType = com.stratio.streaming.commons.constants.ColumnType.STRING;
        // break;
        // default:
        // throw new UnsupportedException("Column type " + columnType.name() + " not supported in Streaming");
        // }
        return colType;
    }

}
