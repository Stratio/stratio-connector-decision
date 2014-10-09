package com.stratio.connector.streaming.core.engine.query.queryExecutor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.stratio.connector.streaming.core.engine.query.ConnectorQueryData;
import com.stratio.connector.streaming.core.engine.query.util.StreamResultSet;
import com.stratio.meta.common.connector.IResultHandler;
import com.stratio.meta.common.data.Cell;
import com.stratio.meta.common.data.ResultSet;
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

    private List<Row> list = Collections.synchronizedList(new ArrayList());

    /**
     * @param queryData
     * @param resultHandler
     */
    public ElementNumberQueryExecutor(ConnectorQueryData queryData, IResultHandler resultHandler) {
        super(queryData,resultHandler);
        windowLength = queryData.getWindow().numOfElement;
        // TODO SetWindowLength Auto-generated constructor stub
    }

    @Override
    protected void processMessage(StratioStreamingMessage theMessage) {

        Row row = new Row();

        for (ColumnNameTypeValue column : theMessage.getColumns()) {
            System.out.print(" Column: " + column.getColumn());
            System.out.print(" || Type: " + column.getType());
            System.out.print(" || Value: " + column.getValue());

            row.addCell(column.getColumn(), new Cell(column.getValue()));
        }

        synchronized (list){ //TODO ver si se puede sincronizar menos
            list.add(row);
            if (list.size() == windowLength){

                ResultSet resultSet = new ResultSet();
                ArrayList<Row> copyNotSyncrhonizedList;

                copyNotSyncrhonizedList = new ArrayList<>(list);
                list.clear();
                resultSet.setRows(copyNotSyncrhonizedList);
                QueryResult result = QueryResult.createQueryResult(resultSet);
                resultHandler.processResult(result);
                }


            }
        }




    }

