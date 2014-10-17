package com.stratio.connector.streaming.core.procces; 

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Test;
import org.junit.Before; 
import org.junit.After;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.internal.util.reflection.Whitebox;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.stratio.connector.commons.connection.Connection;
import com.stratio.connector.streaming.core.engine.query.ConnectorQueryBuilder;
import com.stratio.connector.streaming.core.engine.query.ConnectorQueryData;
import com.stratio.connector.streaming.core.engine.query.ConnectorQueryParser;
import com.stratio.connector.streaming.core.engine.query.queryExecutor.ConnectorQueryExecutor;
import com.stratio.meta.common.connector.IResultHandler;
import com.stratio.meta.common.logicalplan.Project;
import com.stratio.meta2.common.data.TableName;
import com.stratio.streaming.api.IStratioStreamingAPI;

/** 
* QueryProcess Tester. 
* 
* @author <Authors name> 
* @since <pre>oct 17, 2014</pre> 
* @version 1.0 
*/
@RunWith(PowerMockRunner.class)
@PrepareForTest({ConnectorQueryExecutor.class,QueryProcess.class})
public class QueryProcessTest {

    private static final String CATALOG = "catalog";
    private static final String TABLE = "table";
    String QUERY = "query";
    @Mock Connection<IStratioStreamingAPI> CONNECTION ;
    @Mock IResultHandler RESULT_HANDLER;
    @Mock String QUERY_ID = "QUERY_ID";
      @Mock Project PROJECT;
    QueryProcess queryProcess;
    @Mock IStratioStreamingAPI streamingApi;

    @Mock ConnectorQueryParser queryParser;
    @Mock ConnectorQueryBuilder queryBuilder;
    @Mock ConnectorQueryExecutor connectorQueryExecutor;

    @Before
public void before() throws Exception {

        when(CONNECTION.getNativeConnection()).thenReturn(streamingApi);
        queryProcess = new QueryProcess(QUERY_ID,PROJECT,RESULT_HANDLER,CONNECTION);
        Whitebox.setInternalState(queryProcess, "queryParser", queryParser);
        Whitebox.setInternalState(queryProcess,"queryBuilder",queryBuilder);





} 

/** 
* 
* Method: run()
* 
*/
@Test
public void testRun() throws Exception {

    ConnectorQueryData connectorQueryData = mock(ConnectorQueryData.class);
    when(queryParser.transformLogicalWorkFlow(PROJECT, QUERY_ID)).thenReturn(connectorQueryData);
    when(queryBuilder.createQuery(connectorQueryData)).thenReturn(QUERY);
    PowerMockito.whenNew(ConnectorQueryExecutor.class).withArguments(connectorQueryData,
            RESULT_HANDLER).thenReturn(connectorQueryExecutor);
    queryProcess.run();

    verify(connectorQueryExecutor,times(1)).executeQuery(QUERY,CONNECTION);
} 

/** 
* 
* Method: endQuery() 
* 
*/ 
@Test
public void testEndQuery() throws Exception {

    Whitebox.setInternalState(queryProcess,"queryExecutor",connectorQueryExecutor);
    TableName tableName = new TableName(CATALOG,TABLE);
    when(PROJECT.getTableName()).thenReturn(tableName);

    queryProcess.endQuery();

    verify(connectorQueryExecutor,times(1)).endQuery(CATALOG+"_"+TABLE,CONNECTION);
} 



} 