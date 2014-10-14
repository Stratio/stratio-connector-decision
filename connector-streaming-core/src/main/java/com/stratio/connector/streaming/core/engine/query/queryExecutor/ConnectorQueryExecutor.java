package com.stratio.connector.streaming.core.engine.query.queryExecutor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import kafka.consumer.KafkaStream;
import kafka.message.MessageAndMetadata;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stratio.connector.commons.connection.Connection;
import com.stratio.connector.streaming.core.engine.query.ConnectorQueryData;
import com.stratio.connector.streaming.core.engine.query.util.StreamUtil;
import com.stratio.meta.common.connector.IResultHandler;
import com.stratio.meta.common.data.Cell;
import com.stratio.meta.common.data.ResultSet;
import com.stratio.meta.common.data.Row;
import com.stratio.meta.common.exceptions.UnsupportedException;
import com.stratio.meta.common.logicalplan.Project;
import com.stratio.meta.common.logicalplan.Select;
import com.stratio.meta.common.metadata.structures.ColumnMetadata;
import com.stratio.meta.common.result.QueryResult;
import com.stratio.meta2.common.data.ColumnName;
import com.stratio.meta2.common.metadata.ColumnType;
import com.stratio.streaming.api.IStratioStreamingAPI;
import com.stratio.streaming.commons.exceptions.StratioAPISecurityException;
import com.stratio.streaming.commons.exceptions.StratioEngineOperationException;
import com.stratio.streaming.commons.exceptions.StratioEngineStatusException;
import com.stratio.streaming.commons.messages.ColumnNameTypeValue;
import com.stratio.streaming.commons.messages.StratioStreamingMessage;

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
    List<ColumnMetadata> columnsMetadata;
    List<Integer> rowOrder;

    public ConnectorQueryExecutor(ConnectorQueryData queryData, IResultHandler resultHandler)
                    throws UnsupportedException {
        this.queryData = queryData;
        this.resultHandler = resultHandler;
        rowOrder = new ArrayList<Integer>();
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

        StreamUtil.insertRandomData(stratioStreamingAPI, streamOutgoingName, queryData.getSelect());
        logger.info("Waiting a message...");
        for (MessageAndMetadata stream : streams) {
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
     * @throws UnsupportedException
     * 
     */
    private void setColumnMetadata() throws UnsupportedException {
        columnsMetadata = new ArrayList<>();
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
     * @throws UnsupportedException
     */
    private ColumnType updateColumnType(ColumnType colType) throws UnsupportedException {
        switch (colType) {

        case BIGINT:
            colType = ColumnType.DOUBLE;
            break;
        case BOOLEAN:
            colType = ColumnType.BOOLEAN;
            break;
        case DOUBLE:
            colType = ColumnType.DOUBLE;
            break;
        case FLOAT:
            colType = ColumnType.DOUBLE;
            break;
        case INT:
            colType = ColumnType.DOUBLE;
            break;
        case TEXT:
        case VARCHAR:
            colType = ColumnType.VARCHAR;
            break;
        default:
            throw new UnsupportedException("Column type " + colType.name() + " not supported in Streaming");
        }
        return colType;
    }

    protected void sendResultSet(List<Row> copyNotSyncrhonizedList) {
        ResultSet resultSet = new ResultSet();
        resultSet.setColumnMetadata(this.columnsMetadata);
        resultSet.setRows(copyNotSyncrhonizedList);
        QueryResult result = QueryResult.createQueryResult(resultSet);
        result.setQueryId(queryData.getQueryId());
        resultHandler.processResult(result);
    }

    protected Row getSortRow(List<ColumnNameTypeValue> columns) {

        Row row = new Row();
        for (ColumnNameTypeValue column : columns) {
            row.addCell(column.getColumn(), new Cell(column.getValue()));
        }

        // if (rowOrder != null)
        // setRowOrder(columns);
        //
        // for (Integer rowElement : rowOrder) {
        // ColumnNameTypeValue column = columns.get(rowElement);
        //
        // }

        return row;

    }

    /**
     * @param columns
     * 
     */
    private void setRowOrder(List<ColumnNameTypeValue> columns) {

        Select select = queryData.getSelect();
        Collection<String> aliases = select.getColumnMap().values();
        List<String> columnsReceived = new ArrayList<String>();

        for (ColumnNameTypeValue column : columns) {
            columnsReceived.add(column.getColumn());
        }

        for (String alias : aliases) {
            rowOrder.add(columnsReceived.indexOf(alias));
        }
    }


}
