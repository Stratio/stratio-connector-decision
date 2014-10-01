/*
 * Stratio Meta
 *
 *   Copyright (c) 2014, Stratio, All rights reserved.
 *
 *   This library is free software; you can redistribute it and/or modify it under the terms of the
 *   GNU Lesser General Public License as published by the Free Software Foundation; either version
 *   3.0 of the License, or (at your option) any later version.
 *
 *   This library is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 *   even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 *   Lesser General Public License for more details.
 *
 *   You should have received a copy of the GNU Lesser General Public License along with this library.
 */

package com.stratio.connector.streaming.ftest.functionalInsert;

import java.util.HashMap;
import java.util.Map;

import com.stratio.connector.commons.ftest.schema.TableMetadataBuilder;
import com.stratio.connector.streaming.core.StreamingConnector;
import com.stratio.meta.common.connector.ConnectorClusterConfig;
import com.stratio.meta.common.data.Cell;
import com.stratio.meta.common.data.Row;
import com.stratio.meta.common.exceptions.ConnectionException;
import com.stratio.meta.common.exceptions.ExecutionException;
import com.stratio.meta.common.exceptions.UnsupportedException;
import com.stratio.meta2.common.data.ClusterName;
import com.stratio.meta2.common.metadata.ColumnType;
import com.stratio.meta2.common.metadata.TableMetadata;

/**
 * Created by jmgomez on 4/09/14.
 */
public class StreamingBulkInsertMain {

    protected static int getRowToInsert() {
        return 10000;
    }

    public static final String COLUMN_1 = "name1";
    public static final String COLUMN_2 = "name2";
    public static final String COLUMN_3 = "name3";
    public static final String VALUE_1 = "value1_R";
    public static final String VALUE_2 = "value2_R";
    public static final String VALUE_3 = "value3_R";
    public static final int DEFAULT_ROWS_TO_INSERT = 100;
    public static final String COLUMN_KEY = "key";

    public static void main(String args[]) { // TODO

        try {
            String ZOOKEEPER_SERVER = "10.200.0.58"; // "10.200.0.58";//"192.168.0.2";
            String KAFKA_SERVER = "10.200.0.58";// "10.200.0.58";//"192.168.0.2";
            String KAFKA_PORT = "9092";
            String ZOOKEEPER_PORT = "2181";

            StreamingConnector sC = new StreamingConnector();
            sC.init(null);

            Map<String, String> optionsNode = new HashMap<>();

            optionsNode.put("KafkaServer", KAFKA_SERVER);
            optionsNode.put("KafkaPort", KAFKA_PORT);
            optionsNode.put("zooKeeperServer", ZOOKEEPER_SERVER);
            optionsNode.put("zooKeeperPort", ZOOKEEPER_PORT);

            ClusterName clusterName = new ClusterName("CLUSTERNAME");

            ConnectorClusterConfig config = new ConnectorClusterConfig(new ClusterName("CLUSTERNAME"), optionsNode);

            sC.connect(null, config);

            TableMetadataBuilder tableMetadataBuilder = new TableMetadataBuilder("testC", "testT");
            tableMetadataBuilder.addColumn(COLUMN_KEY, ColumnType.VARCHAR).addColumn(COLUMN_1, ColumnType.VARCHAR)
                            .addColumn(COLUMN_2, ColumnType.VARCHAR).addColumn(COLUMN_3, ColumnType.VARCHAR);

            TableMetadata targetTable = tableMetadataBuilder.build();

            sC.getMetadataEngine().createTable(clusterName, targetTable);

            for (int i = 0; i < getRowToInsert(); i++) {

                Row row = new Row();
                Map<String, Cell> cells = new HashMap();
                cells.put(COLUMN_KEY, new Cell(i));

                String value1 = (i % 2 == 0) ? "50" : "25";
                cells.put(COLUMN_1, new Cell(value1));
                cells.put(COLUMN_2, new Cell(VALUE_2 + i));
                cells.put(COLUMN_3, new Cell(VALUE_3 + i));
                row.setCells(cells);
                sC.getStorageEngine().insert(clusterName, targetTable, row);
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }

            sC.getMetadataEngine().dropTable(clusterName, targetTable.getName());

        } catch (ConnectionException e) {
            e.printStackTrace();
        } catch (UnsupportedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }

    }
}
