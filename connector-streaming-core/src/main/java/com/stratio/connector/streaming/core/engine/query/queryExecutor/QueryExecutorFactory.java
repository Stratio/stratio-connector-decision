package com.stratio.connector.streaming.core.engine.query.queryExecutor;

import com.stratio.connector.streaming.core.engine.query.ConnectorQueryData;
import com.stratio.meta.common.connector.IResultHandler;

/**
 * Created by jmgomez on 7/10/14.
 */
public class QueryExecutorFactory {
    public static ConnectorQueryExecutor getQueryExecutor(ConnectorQueryData queryData, IResultHandler resultHandler) {
        ConnectorQueryExecutor connectorQueryExecutor = null;
        boolean timeWindow = true;
        if (timeWindow) {
            connectorQueryExecutor = new TimeWindowQueryExecutor(queryData,resultHandler);
        } else {
            connectorQueryExecutor = new ElementNumberQueryExecutor(queryData,resultHandler);
        }
        return connectorQueryExecutor;
    }
}
