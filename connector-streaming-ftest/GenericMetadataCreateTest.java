/*
 * Stratio Deep
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
package com.stratio.connector.commons.ftest.functionalMetadata;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Test;

import com.stratio.connector.commons.ftest.GenericConnectorTest;
import com.stratio.meta.common.exceptions.ExecutionException;
import com.stratio.meta.common.exceptions.UnsupportedException;
import com.stratio.meta2.common.data.CatalogName;
import com.stratio.meta2.common.data.ClusterName;
import com.stratio.meta2.common.data.ColumnName;
import com.stratio.meta2.common.data.IndexName;
import com.stratio.meta2.common.data.TableName;
import com.stratio.meta2.common.metadata.CatalogMetadata;
import com.stratio.meta2.common.metadata.ColumnMetadata;
import com.stratio.meta2.common.metadata.ColumnType;
import com.stratio.meta2.common.metadata.IndexMetadata;
import com.stratio.meta2.common.metadata.IndexType;
import com.stratio.meta2.common.metadata.TableMetadata;
import com.stratio.meta2.common.statements.structures.selectors.BooleanSelector;
import com.stratio.meta2.common.statements.structures.selectors.IntegerSelector;
import com.stratio.meta2.common.statements.structures.selectors.Selector;
import com.stratio.meta2.common.statements.structures.selectors.StringSelector;

public abstract class GenericMetadataCreateTest extends GenericConnectorTest {

    private static final String NEW_CATALOG = "new_catalog";
    private static final String INDEX = "index1";

    @Test
    public void createCatalogTest() throws UnsupportedException, ExecutionException {
        ClusterName clusterName = getClusterName();
        System.out.println("*********************************** INIT FUNCTIONAL TEST createCatalogTest "
                        + clusterName.getName() + " ***********************************");

        try {
            connector.getMetadataEngine().dropCatalog(getClusterName(), new CatalogName(NEW_CATALOG));
            fail("When I try to delete a catalog that not exists any type of exception must be throws. It may be a runtime exception.");
        } catch (Throwable t) {
        }

        connector.getMetadataEngine()
                        .createCatalog(getClusterName(),
                                        new CatalogMetadata(new CatalogName(NEW_CATALOG), Collections.EMPTY_MAP,
                                                        Collections.EMPTY_MAP));

        try {
            connector.getMetadataEngine().dropCatalog(getClusterName(), new CatalogName(NEW_CATALOG));

        } catch (Throwable t) {
            fail("Now I drop a catalog that exist. The operation must be correct.");
        }

    }

    @Test
    public void createCatalogWithOptionsTest() throws UnsupportedException, ExecutionException {
        ClusterName clusterName = getClusterName();
        System.out.println("*********************************** INIT FUNCTIONAL TEST createCatalogTest "
                        + clusterName.getName() + " ***********************************");

        try {
            connector.getMetadataEngine().dropCatalog(getClusterName(), new CatalogName(NEW_CATALOG));
            fail("When I try to delete a catalog that not exists any type of exception must be throws. It may be a runtime exception.");
        } catch (Throwable t) {
        }

        Map<Selector, Selector> options = new HashMap<>();
        options.put(new StringSelector("option1"), new StringSelector("value1"));
        options.put(new StringSelector("option2"), new IntegerSelector(new Integer(3)));
        options.put(new StringSelector("option3"), new BooleanSelector(false));
        connector.getMetadataEngine().createCatalog(getClusterName(),
                        new CatalogMetadata(new CatalogName(NEW_CATALOG), options, Collections.EMPTY_MAP));

        Map<String, Object> recoveredSettings = getConnectorHelper().recoveredCatalogSettings(NEW_CATALOG);

        Set<String> keys = recoveredSettings.keySet();
        assertTrue("The option1 exists", keys.contains("option1"));
        assertTrue("The option2 exists", keys.contains("option2"));
        assertTrue("The option3 exists", keys.contains("option3"));

        assertEquals("The value of option1 is correct", recoveredSettings.get("option1"), "value1");
        assertEquals("The value of option2 is correct", recoveredSettings.get("option2"), "3");
        assertEquals("The value of option2 is correct", recoveredSettings.get("option3"), "false");

        try {
            connector.getMetadataEngine().dropCatalog(getClusterName(), new CatalogName(NEW_CATALOG));

        } catch (Throwable t) {
            fail("Now I drop a catalog that exist. The operation must be correct.");
        }

    }

    @Test
    public void createCatalogExceptionCreateTwoCatalogTest() throws UnsupportedException, ExecutionException {
        ClusterName clusterName = getClusterName();
        System.out.println("*********************************** INIT FUNCTIONAL TEST createCatalogTest "
                        + clusterName.getName() + " ***********************************");

        try {
            connector.getMetadataEngine().dropCatalog(getClusterName(), new CatalogName(NEW_CATALOG));
            fail("When I try to delete a catalog that not exists any type of exception must be throws. It may be a runtime exception.");
        } catch (Throwable t) {
        }

        connector.getMetadataEngine()
                        .createCatalog(getClusterName(),
                                        new CatalogMetadata(new CatalogName(NEW_CATALOG), Collections.EMPTY_MAP,
                                                        Collections.EMPTY_MAP));
        try {
            connector.getMetadataEngine().createCatalog(
                            getClusterName(),
                            new CatalogMetadata(new CatalogName(NEW_CATALOG), Collections.EMPTY_MAP,
                                            Collections.EMPTY_MAP));
            fail("I try to create a second catalog with the same identification. Any type of exception must be throws. It may be a runtime excepcion");
        } catch (Throwable t) {

        }

        connector.getMetadataEngine().dropCatalog(getClusterName(), new CatalogName(NEW_CATALOG));

    }

    @Test
    public void createTableWithoutTableTest() throws UnsupportedException, ExecutionException {
        ClusterName clusterName = getClusterName();
        System.out.println("*********************************** INIT FUNCTIONAL TEST createTableTest ***********************************");

        TableName tableName = new TableName(CATALOG, TABLE);
        Map<Selector, Selector> options = Collections.EMPTY_MAP;

        Map<ColumnName, ColumnMetadata> columns = Collections.EMPTY_MAP;
        Map indexex = columns = Collections.EMPTY_MAP;
        ClusterName clusterRef = getClusterName();
        List<ColumnName> partitionKey = Collections.EMPTY_LIST;
        List<ColumnName> clusterKey = Collections.EMPTY_LIST;

        // We must create the catalog firs
        connector.getMetadataEngine().createCatalog(getClusterName(),
                        new CatalogMetadata(new CatalogName(CATALOG), Collections.EMPTY_MAP, Collections.EMPTY_MAP));

        try {
            connector.getMetadataEngine().dropTable(getClusterName(), tableName);
            fail("When I try to delete a table that not exists any type of exception must be throws. It may be a runtime exception.");
        } catch (Throwable t) {
        }

        connector.getMetadataEngine().createTable(getClusterName(),
                        new TableMetadata(tableName, options, columns, indexex, clusterRef, partitionKey, clusterKey));
        try {
            connector.getMetadataEngine().dropTable(getClusterName(), tableName);
        } catch (Throwable t) {
            fail("Now I drop a catalog that exist. The operation must be correct.");
        }

        connector.getMetadataEngine().dropCatalog(getClusterName(), new CatalogName(CATALOG));
    }

    @Test
    public void createTableTest() throws UnsupportedException, ExecutionException {
        ClusterName clusterName = getClusterName();
        System.out.println("*********************************** INIT FUNCTIONAL TEST createTableTest ***********************************");

        TableName tableName = new TableName(CATALOG, TABLE);
        Map<Selector, Selector> options = Collections.EMPTY_MAP;

        Map<ColumnName, ColumnMetadata> columns = new HashMap<>();
        int i = 1;
        Collection<ColumnType> allSupportedColumnType = getConnectorHelper().getAllSupportedColumnType();
        for (ColumnType columnType : allSupportedColumnType) {
            ColumnName columnName = new ColumnName(CATALOG, TABLE, "columnName_" + i);
            columns.put(columnName, new ColumnMetadata(columnName, null, columnType));
            i++;
        }

        Map indexex = Collections.EMPTY_MAP;
        ClusterName clusterRef = getClusterName();
        List<ColumnName> partitionKey = Collections.EMPTY_LIST;
        List<ColumnName> clusterKey = Collections.EMPTY_LIST;

        // We must create the catalog firs
        connector.getMetadataEngine().createCatalog(getClusterName(),
                        new CatalogMetadata(new CatalogName(CATALOG), Collections.EMPTY_MAP, Collections.EMPTY_MAP));

        try {
            connector.getMetadataEngine().dropTable(getClusterName(), tableName);
            fail("When I try to delete a table that not exists any type of exception must be throws. It may be a runtime exception.");
        } catch (Throwable t) {
        }

        connector.getMetadataEngine().createTable(getClusterName(),
                        new TableMetadata(tableName, options, columns, indexex, clusterRef, partitionKey, clusterKey));
        try {
            connector.getMetadataEngine().dropTable(getClusterName(), tableName);
        } catch (Throwable t) {
            fail("Now I drop a catalog that exist. The operation must be correct.");
        }

        connector.getMetadataEngine().dropCatalog(getClusterName(), new CatalogName(CATALOG));

    }

    @Test
    public void createCatalogWithTablesAndIndex() throws UnsupportedException, ExecutionException {
        ClusterName clusterName = getClusterName();
        System.out.println("*********************************** INIT FUNCTIONAL TEST createCatalogWithTablesAndIndexTest ***********************************");

        TableName tableName = new TableName(CATALOG, TABLE);
        ClusterName clusterRef = getClusterName();
        List<ColumnName> partitionKey = Collections.EMPTY_LIST;
        List<ColumnName> clusterKey = Collections.EMPTY_LIST;

        // ColumnMetadata (all columns)
        Map<Selector, Selector> options = Collections.EMPTY_MAP;
        Map<ColumnName, ColumnMetadata> columnsMap = new HashMap<>();
        int i = 1;
        Collection<ColumnType> allSupportedColumnType = getConnectorHelper().getAllSupportedColumnType();
        for (ColumnType columnType : allSupportedColumnType) {
            ColumnName columnName = new ColumnName(CATALOG, TABLE, "columnName_" + i);
            columnsMap.put(columnName, new ColumnMetadata(columnName, null, columnType));
            i++;
        }

        // ColumnMetadata (list of columns to create a single index)
        List<ColumnMetadata> columns = new ArrayList<>();
        Object[] parameters = null;
        columns.add(new ColumnMetadata(new ColumnName(tableName, "columnName_1"), parameters, ColumnType.TEXT));

        // Creating the index with the previous columns
        Map<IndexName, IndexMetadata> indexMap = new HashMap<IndexName, IndexMetadata>();
        indexMap.put(new IndexName(tableName, INDEX), new IndexMetadata(new IndexName(tableName, INDEX), columns,
                        IndexType.DEFAULT, options));

        Map<TableName, TableMetadata> tableMap = new HashMap<TableName, TableMetadata>();
        TableMetadata tableMetadata = new TableMetadata(tableName, options, columnsMap, indexMap, clusterRef,
                        partitionKey, clusterKey);
        tableMap.put(tableName, tableMetadata);

        assertFalse(iConnectorHelper.containsIndex(CATALOG, TABLE, INDEX));

        connector.getMetadataEngine().createCatalog(getClusterName(),
                        new CatalogMetadata(new CatalogName(CATALOG), Collections.EMPTY_MAP, tableMap));
        connector.getMetadataEngine().createTable(getClusterName(), tableMetadata);

        assertTrue(iConnectorHelper.containsIndex(CATALOG, TABLE, INDEX));

        connector.getMetadataEngine().dropCatalog(getClusterName(), new CatalogName(CATALOG));

        // check if when the catalog is dropped, all the meta-info is removed
        assertFalse(iConnectorHelper.containsIndex(CATALOG, TABLE, INDEX));

    }

}