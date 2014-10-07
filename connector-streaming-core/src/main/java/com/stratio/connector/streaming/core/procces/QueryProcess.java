package com.stratio.connector.streaming.core.procces;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stratio.connector.commons.connection.Connection;
import com.stratio.connector.streaming.core.engine.query.ConnectorQueryBuilder;
import com.stratio.connector.streaming.core.engine.query.ConnectorQueryData;

import com.stratio.connector.streaming.core.engine.query.ConnectorQueryParser;

import com.stratio.meta.common.connector.IResultHandler;
import com.stratio.meta.common.exceptions.ExecutionException;
import com.stratio.meta.common.exceptions.UnsupportedException;
import com.stratio.meta.common.logicalplan.Project;
import com.stratio.streaming.api.IStratioStreamingAPI;

import com.stratio.streaming.commons.exceptions.StratioAPISecurityException;
import com.stratio.streaming.commons.exceptions.StratioEngineOperationException;
import com.stratio.streaming.commons.exceptions.StratioEngineStatusException;
import com.stratio.connector.streaming.core.engine.query.util.StreamUtil;
import com.stratio.connector.streaming.core.engine.query.queryExecutor.ConnectorQueryExecutor;
import com.stratio.connector.streaming.core.engine.query.queryExecutor.QueryExecutorFactory;


/**
 * Created by jmgomez on 3/10/14.
 */
public class QueryProcess implements ConnectorProcess{
    /**
     * The log.
     */
    final transient Logger logger = LoggerFactory.getLogger(this.getClass());
    private  String queryId;

    private  Project project;
    private  IResultHandler resultHandler;
    private Connection<IStratioStreamingAPI> connection;
    private  ConnectorQueryExecutor queryExecutor;





    public QueryProcess(String queryId, Project project, IResultHandler resultHandler, Connection connection) {
        this.project = project;
        this.resultHandler = resultHandler;
        this.connection = connection;
        this.queryId = queryId;
    }

    public void run(){
        try {

            ConnectorQueryParser queryParser = new ConnectorQueryParser();
            ConnectorQueryData queryData = queryParser.transformLogicalWorkFlow(project,queryId);
            ConnectorQueryBuilder queryBuilder = new ConnectorQueryBuilder(queryData);
            String query = queryBuilder.createQuery();
            if (logger.isDebugEnabled()) {
                logger.debug("The streaming query is: [" + query + "]");

            }


            queryExecutor = QueryExecutorFactory.getQueryExecutor(queryData);

            queryExecutor.executeQuery(query, connection, queryData,resultHandler);



        } catch (StratioEngineStatusException | StratioAPISecurityException | StratioEngineOperationException |
                UnsupportedException |ExecutionException e) {
            String msg = "Streaming query execution fail." + e.getMessage();
            logger.error(msg);
            resultHandler.processException(queryId,new ExecutionException(msg,e));

        }catch (InterruptedException e){
            logger.info("The query is stop");
        }
    }

    @Override public void endQuery() {
    	
    	try {
            String streamName = StreamUtil.createStreamName(project.getTableName());
    		queryExecutor.endQuery(streamName, connection);
		} catch (StratioEngineStatusException | StratioAPISecurityException | StratioEngineOperationException e) {
			String msg = "Streaming query stop fail." + e.getMessage();
            logger.error(msg);
            resultHandler.processException(queryId,new ExecutionException(msg,e));
		}
       
    }

	@Override
	public Project getProject() {
		// TODO Auto-generated method stub
		return project;
	}


}
