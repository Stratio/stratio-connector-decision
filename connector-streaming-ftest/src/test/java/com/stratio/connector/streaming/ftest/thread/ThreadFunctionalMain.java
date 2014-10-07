package com.stratio.connector.streaming.ftest.thread;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import com.stratio.connector.commons.ftest.workFlow.LogicalWorkFlowCreator;
import com.stratio.connector.streaming.core.StreamingConnector;
import com.stratio.connector.streaming.ftest.thread.actions.StreamingInserter;
import com.stratio.connector.streaming.ftest.thread.actions.StreamingRead;
import com.stratio.meta.common.connector.ConnectorClusterConfig;
import com.stratio.meta.common.connector.IResultHandler;
import com.stratio.meta.common.exceptions.ConnectionException;
import com.stratio.meta.common.exceptions.ExecutionException;
import com.stratio.meta.common.exceptions.UnsupportedException;
import com.stratio.meta.common.logicalplan.LogicalWorkflow;
import com.stratio.meta.common.result.QueryResult;
import com.stratio.meta2.common.data.ClusterName;
import com.stratio.meta2.common.data.ColumnName;
import com.stratio.meta2.common.data.TableName;
import com.stratio.meta2.common.metadata.ColumnMetadata;
import com.stratio.meta2.common.metadata.ColumnType;
import com.stratio.meta2.common.metadata.TableMetadata;

public class ThreadFunctionalMain {

    public static void main(String[] args) throws ConnectionException, UnsupportedException, InterruptedException {
        final String OTHER_TEXT = "OTHER...... ";
        final int WAIT_TIME = 40000;
        final String CATALOG_NAME = "_name";
        final String TABLE_NAME = "table_name";
        String ZOOKEEPER_SERVER = "10.200.0.58";
        ;// "192.168.0.2";
        String KAFKA_SERVER = "10.200.0.58";// "192.168.0.2";
        String KAFKA_PORT = "9092";
        String ZOOKEEPER_PORT = "2181";
        String STRING_COLUMN = "string_column";
        String INTEGER_COLUMN = "integer_column";
        String BOOLEAN_COLUMN = "boolean_column";

        StreamingConnector sC;
        TableMetadata tableMetadata;

        ClusterName clusterName = new ClusterName("CLUSTERNAME");

        Boolean correct = true;

        sC = new StreamingConnector();

        sC.init(null);

        Map<String, String> optionsNode = new HashMap<>();

        optionsNode.put("KafkaServer", KAFKA_SERVER);
        optionsNode.put("KafkaPort", KAFKA_PORT);
        optionsNode.put("zooKeeperServer", ZOOKEEPER_SERVER);
        optionsNode.put("zooKeeperPort", ZOOKEEPER_PORT);

        ConnectorClusterConfig config = new ConnectorClusterConfig(clusterName, optionsNode);
        sC.connect(null, config);
        TableName tableName = new TableName(CATALOG_NAME, TABLE_NAME);
        Map<ColumnName, ColumnMetadata> columns = new LinkedHashMap<>();
        ColumnName columnNameString = new ColumnName(tableName, STRING_COLUMN);
        columns.put(columnNameString, new ColumnMetadata(columnNameString, new Object[] {}, ColumnType.VARCHAR));
        ColumnName columnNameInteger = new ColumnName(tableName, INTEGER_COLUMN);
        columns.put(columnNameInteger, new ColumnMetadata(columnNameInteger, new Object[] {}, ColumnType.INT));
        ColumnName columnNameBoolean = new ColumnName(tableName, BOOLEAN_COLUMN);
        columns.put(columnNameBoolean, new ColumnMetadata(columnNameBoolean, new Object[] {}, ColumnType.BOOLEAN));
        tableMetadata = new TableMetadata(tableName, Collections.EMPTY_MAP, columns, Collections.EMPTY_MAP,
                        clusterName, Collections.EMPTY_LIST, Collections.EMPTY_LIST);

        try {
            sC.getMetadataEngine().createTable(clusterName, tableMetadata);
        } catch (ExecutionException t) {

        }

        System.out.println("Thread Query ID " + Thread.currentThread().getId());
        System.out.println("TEST ********************** Inserting ......");
        StreamingInserter stramingInserter = new StreamingInserter(sC, clusterName, tableMetadata);
        stramingInserter.start();

        LogicalWorkflow logicalWokflow = new LogicalWorkFlowCreator(CATALOG_NAME, TABLE_NAME, clusterName)
                        .addColumnName(STRING_COLUMN).addColumnName(INTEGER_COLUMN).addColumnName(BOOLEAN_COLUMN)
                        .getLogicalWorkflow();

        StreamingRead stremingRead = new StreamingRead(sC, clusterName, tableMetadata, logicalWokflow,
                        new IResultHandler() {

                            @Override
                            public void processResult(QueryResult result) {
                                // TODO Auto-generated method stub

                            }

                            @Override
                            public void processException(String queryId, ExecutionException exception) {
                                // TODO Auto-generated method stub

                            }
                        });

        stremingRead.start();
        System.out.println("TEST ********************** Quering......");
        Thread.sleep(4 * WAIT_TIME);

        stremingRead.end();
        System.out.println("TEST ********************** END Quering Test......");
        Thread.sleep(WAIT_TIME);
        System.out.println("TEST ********************** Change Test Quering......");

        stramingInserter.changeOtuput(OTHER_TEXT);
        Thread.sleep(WAIT_TIME);

        System.out.println("TEST ********************** END Insert......");
        stramingInserter.interrupt();
        Thread.sleep(WAIT_TIME);
        System.out.println("End");
        sC.close(clusterName);

    }
}
