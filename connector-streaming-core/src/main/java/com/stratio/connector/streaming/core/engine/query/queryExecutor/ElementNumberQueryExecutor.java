package com.stratio.connector.streaming.core.engine.query.queryExecutor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.stratio.connector.streaming.core.engine.query.ConnectorQueryData;
import com.stratio.meta.common.connector.IResultHandler;
import com.stratio.meta.common.data.Row;
import com.stratio.meta.common.exceptions.UnsupportedException;

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
     * @throws UnsupportedException
     */
    public ElementNumberQueryExecutor(ConnectorQueryData queryData, IResultHandler resultHandler)
                    throws UnsupportedException {
        super(queryData, resultHandler);

        windowLength = queryData.getWindow().getNumRows();



    }

    @Override
    protected void processMessage(Row row) {



        boolean isWindowReady = false;
        ArrayList<Row> copyNotSyncrhonizedList = null;
        synchronized (list) { // TODO ver si se puede sincronizar menos
            list.add(row);
            isWindowReady = (list.size() == windowLength);
            if (isWindowReady) {
                copyNotSyncrhonizedList = new ArrayList<>(list);
                list.clear();
            }
        }

        if (isWindowReady) {
            sendResultSet(copyNotSyncrhonizedList);
        }
    }

}
