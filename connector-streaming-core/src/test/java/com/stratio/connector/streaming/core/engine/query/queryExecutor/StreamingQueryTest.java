package com.stratio.connector.streaming.core.engine.query.queryExecutor; 

import org.junit.Test; 
import org.junit.Before; 
import org.junit.After;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.modules.junit4.PowerMockRunner;

import com.stratio.connector.streaming.core.engine.query.ConnectorQueryData;
import com.stratio.meta.common.connector.Operations;
import com.stratio.meta.common.logicalplan.Project;
import com.stratio.meta.common.logicalplan.Select;
import com.stratio.meta2.common.data.ClusterName;
import com.stratio.meta2.common.data.ColumnName;
import com.stratio.meta2.common.data.TableName;
import com.stratio.meta2.common.metadata.ColumnType;
import com.stratio.streaming.api.IStratioStreamingAPI;
import com.stratio.streaming.commons.messages.StratioStreamingMessage;

import static junit.framework.TestCase.assertEquals;
import static org.mockito.Mockito.*;

import java.util.LinkedHashMap;
import java.util.Map;

import clover.org.apache.commons.collections.map.LinkedMap;
import kafka.consumer.KafkaStream;

/** 
* StreamingQuery Tester. 
* 
* @author <Authors name> 
* @since <pre>oct 15, 2014</pre> 
* @version 1.0 
*/
@RunWith(PowerMockRunner.class)
public class StreamingQueryTest {

    private static final String QUERY = "query";
    private static final String CATALOG = "catalog";
    private static final String TABLE = "table";
    private static final com.stratio.meta2.common.data.ClusterName CLUSTER_NAME = new ClusterName("CLUSTER_NAME");
    private static final String QUERY_ID = "queryId";
    private static final String OUTPUT_STREAM = "output_stream";
    private static final String COLUMN1 = "column_1";
    private static final String ALIAS1 = "alias_1";

    StreamingQuery streamingQuery;

    @Mock IStratioStreamingAPI stratioStreamingApi;
@Before
public void before() throws Exception {
    streamingQuery = new StreamingQuery(createQueryData());
} 

@After
public void after() throws Exception { 
} 

/** 
* 
* Method: createQuery(String query, IStratioStreamingAPI stratioStreamingAPI, ConnectorQueryData queryData) 
* 
*/ 
@Test
public void testCreateQuery() throws Exception {


    String queryId = streamingQuery.createQuery(QUERY,stratioStreamingApi);


    verify(stratioStreamingApi,times(1)).addQuery("catalog_table",QUERY);
    assertEquals("The querySend id is correct","catalog_table_queryId",queryId);

}



    @Test
    public void testListenQurey() throws Exception {

        KafkaStream<String, StratioStreamingMessage> kafkaSrteam = mock(KafkaStream.class);
        when(stratioStreamingApi.listenStream(OUTPUT_STREAM)).thenReturn(kafkaSrteam);

        KafkaStream<String, StratioStreamingMessage> returnKafkaStream = streamingQuery.listenQurey(stratioStreamingApi,
                OUTPUT_STREAM);


        assertEquals("The kafkastream is correct",kafkaSrteam,returnKafkaStream);


    }


    private ConnectorQueryData createQueryData() {
        ConnectorQueryData queryData = new ConnectorQueryData();
        queryData.setProjection(new Project(Operations.PROJECT,new TableName(CATALOG,TABLE),CLUSTER_NAME));
        queryData.setQueryId(QUERY_ID);
        Map<ColumnName, String> columnMap = new LinkedHashMap<>();
        columnMap.put(new ColumnName(CATALOG,TABLE,COLUMN1), ALIAS1);
        Map<String, ColumnType> typemap = new LinkedMap();
        typemap.put(CATALOG+"."+TABLE+"."+COLUMN1, ColumnType.INT);
        Select select = new Select(Operations.SELECT_OPERATOR,columnMap,typemap);
        queryData.setSelect(select);
        return queryData;
    }


} 
