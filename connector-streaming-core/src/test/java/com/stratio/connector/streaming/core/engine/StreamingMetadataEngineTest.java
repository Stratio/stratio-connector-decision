package com.stratio.connector.streaming.core.engine;

import static org.mockito.Matchers.anyList;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.internal.util.reflection.Whitebox;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.stratio.connector.commons.connection.Connection;
import com.stratio.connector.commons.connection.ConnectionHandler;
import com.stratio.connector.streaming.core.engine.query.ConnectorQueryBuilder;
import com.stratio.connector.streaming.core.engine.query.ConnectorQueryParser;
import com.stratio.connector.streaming.core.engine.query.queryExecutor.ConnectorQueryExecutor;
import com.stratio.meta.common.exceptions.UnsupportedException;
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
import org.powermock.api.mockito.PowerMockito;
/**
 * StreamingMetadataEngine Tester.
 *
 * @author <Authors name>
 * @version 1.0
 * @since <pre>oct 16, 2014</pre>
 */
@RunWith(PowerMockRunner.class)

public class StreamingMetadataEngineTest {

    private static final String CATALOG = "catalog";
    private static final String TABLE = "table";
    private static final String CLUSTER_NAME = "cluster_name";
    private static final String COLUM = "column";
    StreamingMetadataEngine streamingMetadataEngine;
    @Mock ConnectionHandler connectionHandler;
    @Mock Connection<IStratioStreamingAPI> connection;
    @Mock com.stratio.streaming.api.IStratioStreamingAPI streamingApi;



    @Before
    public void before() throws Exception {

        when(connection.getNativeConnection()).thenReturn(streamingApi);
        streamingMetadataEngine = new StreamingMetadataEngine(connectionHandler);


    }

    /**
     * Method: createCatalog(CatalogMetadata indexMetaData, Connection<IStratioStreamingAPI> connection)
     */
    @Test(expected = UnsupportedException.class)
    public void testCreateCatalog() throws Exception {
        streamingMetadataEngine.createCatalog(null, (Connection) null);
    }

    /**
     * Method: createTable(TableMetadata streamMetadata, Connection<IStratioStreamingAPI> connection)
     */
    @Test
    public void testCreateTable() throws Exception {

        Map<Selector, Selector> options = Collections.EMPTY_MAP;
        Map<IndexName, IndexMetadata> index = Collections.EMPTY_MAP;
        Map<ColumnName, ColumnMetadata> columns = new LinkedHashMap<>();
        ColumnMetadata columnMetadata = new ColumnMetadata(new ColumnName(CATALOG, TABLE, COLUM), new Object[0],
                ColumnType.INT);
        columns.put(new ColumnName(CATALOG, TABLE, COLUM), columnMetadata);

        List<ColumnName> partitionKey = Collections.EMPTY_LIST;
        List<ColumnName> clusterKey = Collections.EMPTY_LIST;
        TableMetadata tableMetadata = new TableMetadata(true, new TableName(CATALOG, TABLE), options, columns, index,
                new ClusterName(CLUSTER_NAME), partitionKey, clusterKey);
        streamingMetadataEngine.createTable(tableMetadata, connection);

        verify(streamingApi, times(1)).createStream(eq(CATALOG + "_" + TABLE), anyList());
    }

    /**
     * Method: dropCatalog(CatalogName indexName, Connection<IStratioStreamingAPI> connection)
     */
    @Test(expected = UnsupportedException.class)
    public void testDropCatalog() throws Exception {
        streamingMetadataEngine.dropCatalog(null, (Connection) null);

    }

    /**
     * Method: dropTable(TableName stream, Connection<IStratioStreamingAPI> connection)
     */
    @Test
    public void testDropTable() throws Exception {

        streamingMetadataEngine.dropTable(new TableName(CATALOG, TABLE), connection);

        verify(streamingApi, times(1)).dropStream(CATALOG + "_" + TABLE);
    }

    /**
     * Method: createIndex(IndexMetadata indexMetadata, Connection<IStratioStreamingAPI> connection)
     */
    @Test(expected = UnsupportedException.class)
    public void testCreateIndex() throws Exception {
        streamingMetadataEngine.createIndex(null, (Connection) null);
    }

    /**
     * Method: dropIndex(IndexMetadata indexMetadata, Connection<IStratioStreamingAPI> connection)
     */
    @Test(expected = UnsupportedException.class)
    public void testDropIndex() throws Exception {
        streamingMetadataEngine.dropIndex(null, (Connection) null);
    }

} 