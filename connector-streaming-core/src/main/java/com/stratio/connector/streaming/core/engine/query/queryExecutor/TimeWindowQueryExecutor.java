package com.stratio.connector.streaming.core.engine.query.queryExecutor;

import com.stratio.connector.streaming.core.engine.query.ConnectorQueryData;
import com.stratio.connector.streaming.core.engine.query.util.StreamResultSet;
import com.stratio.meta.common.connector.IResultHandler;
import com.stratio.meta.common.data.Cell;
import com.stratio.meta.common.data.Row;
import com.stratio.meta.common.result.QueryResult;
import com.stratio.streaming.commons.messages.ColumnNameTypeValue;
import com.stratio.streaming.commons.messages.StratioStreamingMessage;

/**
 * Created by jmgomez on 7/10/14.
 */
public class TimeWindowQueryExecutor extends ConnectorQueryExecutor {

    private long timestamp;

    /**
     * @param queryData
     */
    public TimeWindowQueryExecutor(ConnectorQueryData queryData) {
        super(queryData);
        timestamp = -1;
    }

    @Override
    protected void processMessage(StratioStreamingMessage theMessage, IResultHandler resultHandler) {

        // TODO descartar elementos con un timestamp inferior

        if (timestamp != theMessage.getTimestamp()) {
            if (timestamp != -1) {
                QueryResult queryResult = QueryResult.createQueryResult(streamResultSet.getResultSet());
                resultHandler.processResult(queryResult);
                streamResultSet = new StreamResultSet(queryData);
            }
            timestamp = theMessage.getTimestamp();
        }

        Row row = new Row();

        for (ColumnNameTypeValue column : theMessage.getColumns()) {
            System.out.print(" Column: " + column.getColumn());
            System.out.print(" || Type: " + column.getType());
            System.out.print(" || Value: " + column.getValue());
            // System.out.println("\n--------- (" + i + ") -----------------");
            row.addCell(column.getColumn(), new Cell(column.getValue()));
        }

        streamResultSet.add(row);

    }
}
