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

import org.junit.Test;

import com.stratio.connector.commons.ftest.GenericConnectorTest;
import com.stratio.connector.commons.ftest.helper.IConnectorHelper;
import com.stratio.connector.commons.ftest.schema.TableMetadataBuilder;
import com.stratio.connector.streaming.ftest.helper.StreamingConnectorHelper;
import com.stratio.meta.common.data.Cell;
import com.stratio.meta.common.data.Row;
import com.stratio.meta.common.exceptions.ConnectionException;
import com.stratio.meta.common.exceptions.ExecutionException;
import com.stratio.meta.common.exceptions.InitializationException;
import com.stratio.meta.common.exceptions.UnsupportedException;
import com.stratio.meta.common.exceptions.ValidationException;
import com.stratio.meta2.common.data.ClusterName;
import com.stratio.meta2.common.metadata.ColumnType;
import com.stratio.meta2.common.metadata.TableMetadata;

/**
 * Created by jmgomez on 4/09/14.
 */
public class StreamingBulkInsertTest extends GenericConnectorTest {

    protected int getRowToInsert() {
        return 1000;
    }

    @Override
    protected IConnectorHelper getConnectorHelper() {
        StreamingConnectorHelper streamingConnectorHelper = null;
        try {
            streamingConnectorHelper = new StreamingConnectorHelper(getClusterName());
        } catch (InitializationException e) {
            e.printStackTrace();
        } catch (ConnectionException e) {
            e.printStackTrace();
        }
        return streamingConnectorHelper;
    }

    public static final String COLUMN_1 = "name1";
    public static final String COLUMN_2 = "name2";
    public static final String COLUMN_3 = "name3";
    public static final String VALUE_1 = "value1_R";
    public static final String VALUE_2 = "value2_R";
    public static final String VALUE_3 = "value3_R";
    public static final int DEFAULT_ROWS_TO_INSERT = 100;
    public static final String COLUMN_KEY = "key";

    @Test
    public void testBulkInsertWithPK() throws ExecutionException, ValidationException, UnsupportedOperationException,
                    UnsupportedException {
        ClusterName clusterName = getClusterName();
        System.out.println("*********************************** INIT FUNCTIONAL TEST testBulkInsertWithPK ***********************************");
        insertBulk(clusterName, true);
    }

    private void insertBulk(ClusterName cluesterName, boolean withPK) throws UnsupportedException, ExecutionException {

        TableMetadataBuilder tableMetadataBuilder = new TableMetadataBuilder("testC", "testT");
        tableMetadataBuilder.addColumn(COLUMN_KEY, ColumnType.VARCHAR).addColumn(COLUMN_1, ColumnType.VARCHAR)
                        .addColumn(COLUMN_2, ColumnType.VARCHAR).addColumn(COLUMN_3, ColumnType.VARCHAR);

        TableMetadata targetTable = tableMetadataBuilder.build();

        if (getConnectorHelper().isTableMandatory()) {
            connector.getMetadataEngine().createTable(getClusterName(), targetTable);
        }

        for (int i = 0; i < getRowToInsert(); i++) {

            Row row = new Row();
            Map<String, Cell> cells = new HashMap();
            cells.put(COLUMN_KEY, new Cell(i));
            cells.put(COLUMN_1, new Cell(VALUE_1 + i));
            cells.put(COLUMN_2, new Cell(VALUE_2 + i));
            cells.put(COLUMN_3, new Cell(VALUE_3 + i));
            row.setCells(cells);
            connector.getStorageEngine().insert(cluesterName, targetTable, row);
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }

        refresh(CATALOG);
    }
}
