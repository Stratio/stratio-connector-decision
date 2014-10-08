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
public class ElementNumberQueryExecutor extends ConnectorQueryExecutor {

    /**
     * The window length
     */
    private int windowLength;

    /**
     * @param window
     */
    public ElementNumberQueryExecutor(ConnectorQueryData queryData) {
        super(queryData);
        // TODO SetWindowLength Auto-generated constructor stub
    }

    @Override
    protected void processMessage(StratioStreamingMessage theMessage, IResultHandler resultHandler) {

        Row row = new Row();

        for (ColumnNameTypeValue column : theMessage.getColumns()) {
            // TODO descartar elementos con un timestamp inferior
            System.out.print(" Column: " + column.getColumn());
            System.out.print(" || Type: " + column.getType());
            System.out.print(" || Value: " + column.getValue());
            // System.out.println("\n--------- (" + i + ") -----------------");
            row.addCell(column.getColumn(), new Cell(column.getValue()));
        }

        streamResultSet.add(row);

        if (streamResultSet.size() == windowLength) {
            QueryResult queryResult = QueryResult.createQueryResult(streamResultSet.getResultSet());
            resultHandler.processResult(queryResult);
            streamResultSet = new StreamResultSet(queryData);
        }
    }
}
