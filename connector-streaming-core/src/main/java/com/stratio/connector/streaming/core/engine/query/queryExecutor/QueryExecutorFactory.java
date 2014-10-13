package com.stratio.connector.streaming.core.engine.query.queryExecutor;

import com.stratio.connector.streaming.core.engine.query.ConnectorQueryData;
import com.stratio.meta.common.connector.IResultHandler;
import com.stratio.meta.common.exceptions.UnsupportedException;

/**
 * Created by jmgomez on 7/10/14.
 */
public class QueryExecutorFactory {
    public static ConnectorQueryExecutor getQueryExecutor(ConnectorQueryData queryData, IResultHandler resultHandler)
                    throws UnsupportedException {
        ConnectorQueryExecutor connectorQueryExecutor = null;
        boolean timeWindow = false;
        if (timeWindow) {
            connectorQueryExecutor = new TimeWindowQueryExecutor(queryData, resultHandler);
        } else {
            connectorQueryExecutor = new ElementNumberQueryExecutor(queryData, resultHandler);
        }
        return connectorQueryExecutor;
    }
}
