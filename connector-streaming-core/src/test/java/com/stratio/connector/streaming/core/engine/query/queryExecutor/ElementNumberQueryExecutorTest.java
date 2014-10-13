package com.stratio.connector.streaming.core.engine.query.queryExecutor; 

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;
import org.junit.Before; 
import org.junit.After;
import org.mockito.Mock;

import com.stratio.connector.commons.connection.Connection;
import com.stratio.connector.streaming.core.engine.query.ConnectorQueryData;

import com.stratio.meta.common.connector.IResultHandler;
import com.stratio.meta.common.connector.Operations;
import com.stratio.meta.common.logicalplan.Project;
import com.stratio.meta.common.statements.structures.window.Window;
import com.stratio.meta.common.statements.structures.window.WindowType;
import com.stratio.meta2.common.data.ClusterName;
import com.stratio.meta2.common.data.TableName;
import com.stratio.streaming.api.IStratioStreamingAPI;
import com.stratio.streaming.commons.messages.ColumnNameTypeValue;
import com.stratio.streaming.commons.messages.StratioStreamingMessage;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.message.MessageAndMetadata;

/** 
* ElementNumberQueryExecutor Tester. 
* 
* @author <Authors name> 
* @since <pre>oct 9, 2014</pre> 
* @version 1.0 
*/ 
public class ElementNumberQueryExecutorTest {

    private static final String CATALOG_NAME = "CATALOG_NAME";
    private static final String TABLE_NAME = "TABLE_NAME";
    private static final String CLUSTER_NAME = "CLUSTER_NAME";
    private static final String QUERY_ID = "QUERY_ID";
    private static final String QUERY = "QUERY";
    private static final String STREAM_NAME = "STREAM_NAME";

    @Mock IStratioStreamingAPI stratioStreamingAPI;
    @Mock Connection<IStratioStreamingAPI> connection;
    private int dataToInsert = 1000;

    @Before
public void before() throws Exception {
    when(connection.getNativeConnection()).thenReturn(stratioStreamingAPI);

} 

@After
public void after() throws Exception { 
} 

/** 
* 
* Method: processMessage(StratioStreamingMessage theMessage, IResultHandler resultHandler) 
* 
*/ 
@Test
public void testProcessMessage() throws Exception {
    ConnectorQueryData queryData = createQueryData();

    ElementNumberQueryExecutor elementNumberQueryExecutor = new ElementNumberQueryExecutor(queryData,
            mock(IResultHandler.class));


    ConsumerIterator<String, StratioStreamingMessage> iterator = mock(ConsumerIterator.class);
    Boolean[] next = new Boolean[dataToInsert+1];
    for(int i =0;i<dataToInsert;i++){
        next[i]=true;
    }
    next[dataToInsert+1] = false;
    when(iterator.hasNext()).thenReturn(true, next);


    MessageAndMetadata<String, StratioStreamingMessage> messageAndMetadata = mock(MessageAndMetadata.class);
    List<ColumnNameTypeValue> columns = new ArrayList<>();
    StratioStreamingMessage message = new StratioStreamingMessage(STREAM_NAME, (long) 0,columns);
    when(messageAndMetadata.message()).thenReturn(message);


    when(iterator.next()).thenReturn(messageAndMetadata);
    KafkaStream<String, StratioStreamingMessage> kafkaStream = mock(KafkaStream.class);
    when(kafkaStream.iterator()).thenReturn(iterator);
    when(stratioStreamingAPI.listenStream(CLUSTER_NAME + "_" + TABLE_NAME + "_" + QUERY_ID)).thenReturn(kafkaStream);

    verify( stratioStreamingAPI.addQuery(CLUSTER_NAME+"_"+TABLE_NAME+"_"+QUERY_ID,QUERY), times(1));

    IResultHandler resultHandler = mock(IResultHandler.class);
    elementNumberQueryExecutor.executeQuery(QUERY, connection);
}



    private ConnectorQueryData createQueryData() {
        ConnectorQueryData queryData = new ConnectorQueryData();
        Window window = new Window(WindowType.NUM_ROWS);
        window.setNumRows(10);
        queryData.setWindow(window);
        Project projection = new Project(Operations.PROJECT, new TableName(CATALOG_NAME, TABLE_NAME),
                new ClusterName(CLUSTER_NAME));
        queryData.setProjection(projection);
        queryData.setQueryId(QUERY_ID);
        return queryData;
    }

} 
