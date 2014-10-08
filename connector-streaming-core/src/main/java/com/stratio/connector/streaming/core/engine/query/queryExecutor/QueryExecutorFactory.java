package com.stratio.connector.streaming.core.engine.query.queryExecutor;

import com.stratio.connector.streaming.core.engine.query.ConnectorQueryData;

/**
 * Created by jmgomez on 7/10/14.
 */
public class QueryExecutorFactory {
    public static ConnectorQueryExecutor getQueryExecutor(ConnectorQueryData queryData) {
        ConnectorQueryExecutor connectorQueryExecutor = null;
        boolean timeWindow = true;
        if (timeWindow) {
            connectorQueryExecutor = new TimeWindowQueryExecutor(queryData);
        } else {
            connectorQueryExecutor = new ElementNumberQueryExecutor(queryData);
        }
        return connectorQueryExecutor;
    }
}
