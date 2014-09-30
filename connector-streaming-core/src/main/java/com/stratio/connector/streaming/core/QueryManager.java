package com.stratio.connector.streaming.core;

import java.util.HashMap;

import com.stratio.meta.common.exceptions.ExecutionException;

/**
 * Created by jmgomez on 30/09/14.
 */
public class QueryManager {

    private HashMap<String,String> queryMap = new HashMap();
    public void addQuery(String queryId, String streamingId) throws ExecutionException {
        if (queryMap.containsKey(queryId)){
            throw new ExecutionException("The queryId ["+queryId+"] is alredy in use");
        }
        queryMap.put(queryId,streamingId);

    }
}
