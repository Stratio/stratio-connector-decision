package com.stratio.connector.streaming.core.procces;

import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stratio.connector.commons.connection.Connection;
import com.stratio.connector.streaming.core.procces.exception.ConnectionProcessException;

/**
 * Created by jmgomez on 3/10/14.
 */
public class ConnectorProcessHandler {

    /**
     * The log.
     */
    final transient Logger logger = LoggerFactory.getLogger(this.getClass());

    private HashMap<String, ConnectorProcess> process = new HashMap<>();

    public void addProcess(String queryId, QueryProcess queryProcess) throws ConnectionProcessException {
    if (process.containsKey(queryId)) {
        String msg = "The process with id " + queryId + " already exists ";
        logger.error(msg);
        throw new ConnectionProcessException(msg);
    }
        process.put(queryId,queryProcess);
    }

    public ConnectorProcess getProcess(String queryId) throws ConnectionProcessException {
        if (!process.containsKey(queryId)) {
            String msg = "The process with id " + queryId + " not exists ";
            logger.error(msg);
            throw new ConnectionProcessException(msg);
        }
        return process.get(queryId);
    }

	public void removeProcess(String queryId) {
		process.remove(queryId);
		
	}
}
