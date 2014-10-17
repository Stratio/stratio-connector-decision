package com.stratio.connector.streaming.core.engine; 

import static org.mockito.Matchers.anyList;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.modules.junit4.PowerMockRunner;

import com.stratio.connector.commons.connection.Connection;
import com.stratio.connector.commons.connection.ConnectionHandler;
import com.stratio.meta.common.data.Cell;
import com.stratio.meta.common.data.Row;
import com.stratio.meta2.common.data.ClusterName;
import com.stratio.meta2.common.data.ColumnName;
import com.stratio.meta2.common.data.IndexName;
import com.stratio.meta2.common.data.TableName;
import com.stratio.meta2.common.metadata.ColumnMetadata;
import com.stratio.meta2.common.metadata.ColumnType;
import com.stratio.meta2.common.metadata.IndexMetadata;
import com.stratio.meta2.common.metadata.TableMetadata;
import com.stratio.meta2.common.statements.structures.selectors.Selector;
import com.stratio.streaming.api.IStratioStreamingAPI;

/** 
* StreamingStorageEngine Tester. 
* 
* @author <Authors name> 
* @since <pre>oct 17, 2014</pre> 
* @version 1.0 
*/
@RunWith(PowerMockRunner.class)
public class StreamingStorageEngineTest {

    private static final String CATALOG = "catalog";
    private static final String TABLE = "table";
    private static final String CLUSTER_NAME = "cluster_name";
    private static final String COLUM = "column";
    private static final Object VALUE1 = "value1";
    private static final Object VALUE2 = "value2";

    StreamingStorageEngine streamingStorageEngine;
    @Mock ConnectionHandler connectionHandler;
    @Mock Connection<IStratioStreamingAPI> connection;
    @Mock com.stratio.streaming.api.IStratioStreamingAPI streamingApi;

    @Before
public void before() throws Exception {
        when(connection.getNativeConnection()).thenReturn(streamingApi);
    streamingStorageEngine = new StreamingStorageEngine(connectionHandler);
}


/** 
* 
* Method: insert(TableMetadata targetStream, Row row, Connection<IStratioStreamingAPI> connection) 
* 
*/
@Test
public void testInsert() throws Exception {

    Row row = createRow(VALUE1);


    streamingStorageEngine.insert(createTableMetadata(),  row, connection);


    verify(streamingApi,times(1)).insertData(eq(CATALOG+"_"+TABLE),anyList());

}


    /**
     *
     * Method: insert(TableMetadata targetStream, Collection<Row> rows, Connection<IStratioStreamingAPI> connection)
     *
     */
    @Test
    public void testInsertBulk() throws Exception {



        Collection<Row> rows = new LinkedList<>();
        rows.add(createRow(VALUE1));
        rows.add(createRow(VALUE2));



        streamingStorageEngine.insert(createTableMetadata(),  rows, connection);


        verify(streamingApi,times(2)).insertData(eq(CATALOG+"_"+TABLE),anyList());



    }



    private Row createRow(Object value) {
        Row row = new Row();
        Map<String, Cell> cells = new LinkedHashMap<>();
        cells.put(COLUM,new Cell(value));
        row.setCells(cells);
        return row;
    }

    private TableMetadata createTableMetadata() {
        Map<Selector, Selector> options = Collections.EMPTY_MAP;
        Map<IndexName, IndexMetadata> index = Collections.EMPTY_MAP;
        Map<ColumnName, ColumnMetadata> columns = new LinkedHashMap<>();
        ColumnMetadata columnMetadata = new ColumnMetadata(new ColumnName(CATALOG,TABLE,COLUM),new Object[0], ColumnType.INT);
        columns.put(new ColumnName(CATALOG,TABLE,COLUM),columnMetadata);

        List<ColumnName> partitionKey = Collections.EMPTY_LIST;
        List<ColumnName> clusterKey = Collections.EMPTY_LIST;
        return new TableMetadata(true,new TableName(CATALOG,TABLE),options,columns,index,
                new ClusterName(CLUSTER_NAME),partitionKey,clusterKey);
    }



} 
