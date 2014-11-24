/*
 * Licensed to STRATIO (C) under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information
 * regarding copyright ownership.  The STRATIO (C) licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.stratio.connector.streaming.core.engine.query.util;

import static junit.framework.TestCase.assertEquals;

import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.internal.util.reflection.Whitebox;
import org.powermock.modules.junit4.PowerMockRunner;

import com.stratio.connector.streaming.core.engine.query.ConnectorQueryData;
import com.stratio.crossdata.common.connector.IResultHandler;
import com.stratio.crossdata.common.data.Cell;
import com.stratio.crossdata.common.data.ClusterName;
import com.stratio.crossdata.common.data.ColumnName;
import com.stratio.crossdata.common.data.Row;
import com.stratio.crossdata.common.data.TableName;
import com.stratio.crossdata.common.logicalplan.Project;
import com.stratio.crossdata.common.logicalplan.Select;
import com.stratio.crossdata.common.metadata.ColumnMetadata;
import com.stratio.crossdata.common.metadata.ColumnType;
import com.stratio.crossdata.common.metadata.Operations;
import com.stratio.crossdata.common.result.QueryResult;

/**
 * ResultsetCreator Tester.
 *
 * @author <Authors name>
 * @version 1.0
 * @since <pre>
 * oct 16, 2014
 * </pre>
 */
@RunWith(PowerMockRunner.class)
public class ResultsetCreatorTest {

    private static final String CATALOG = "catalog";
    private static final String TABLE = "table";
    private static final ClusterName CLUSTER_NAME = new ClusterName("CLUSTER_NAME");
    private static final String QUERY_ID = "queryId";
    private static final String VALUE_1_1 = "value_1_1";
    private static final Object VALUE_2_1 = "value_2_1";
    private static final String CELL_1 = "cell_1";
    private static final String CELL_2 = "cell_2";
    private static final Object VALUE_2_2 = "value_2_2";
    private static final String VALUE_1_2 = "value_1_1";
    @Mock
    IResultHandler iResultHandler;
    private ResultsetCreator resultsetCreator;
    private String[] ALIAS = { "alias_1", "alias_2" };
    private String[] NAME = { "name_1", "name_2" };
    private ColumnType[] TYPE = { ColumnType.BOOLEAN, ColumnType.DOUBLE };

    @Before
    public void before() throws Exception {

        resultsetCreator = new ResultsetCreator(createQueryData());
        List<ColumnMetadata> columnMetadata = (List<ColumnMetadata>) Whitebox.getInternalState(resultsetCreator,
                "columnsMetadata");

        valiateMetadata(columnMetadata);
    }

    private void valiateMetadata(List<ColumnMetadata> columnMetadata) {
        int i = 0;
        for (ColumnMetadata metadata : columnMetadata) {
            assertEquals("the alias is correct", ALIAS[i], metadata.getName().getAlias());
            assertEquals("the name is correct", CATALOG + "." + TABLE + "." + NAME[i], metadata.getName().getQualifiedName());
            assertEquals("the type is correct", TYPE[i], metadata.getColumnType());
            i++;
        }
    }

    @After
    public void after() throws Exception {
    }

    /**
     * Method: create(List<Row> rows)
     */
    @Test
    public void testCreateResultSet() throws Exception {

        List<Row> rows = new LinkedList<>();
        rows.add(createRow(VALUE_1_1, VALUE_2_1));
        rows.add(createRow(VALUE_1_2, VALUE_2_2));

        resultsetCreator.create(rows);
        QueryResult returnValue = (QueryResult) Whitebox.getInternalState(resultsetCreator, "queryResult");

        valiateMetadata(returnValue.getResultSet().getColumnMetadata());
        assertEquals("the rows are correct", rows, returnValue.getResultSet().getRows());
        assertEquals("the query id is correct", QUERY_ID, returnValue.getQueryId());

    }

    private Row createRow(String value1, Object value2) {
        Row row = new Row();
        Map<String, Cell> cells = new LinkedHashMap<>();
        cells.put(CELL_1, new Cell(value1));
        cells.put(CELL_2, new Cell(value2));
        row.setCells(cells);

        return row;
    }

    private ConnectorQueryData createQueryData() {
        ConnectorQueryData queryData = new ConnectorQueryData();
        queryData.setProjection(new Project(Operations.PROJECT, new TableName(CATALOG, TABLE), CLUSTER_NAME));
        queryData.setQueryId(QUERY_ID);
        Map<ColumnName, String> columnMap = new LinkedHashMap<>();
        columnMap.put(new ColumnName(CATALOG, TABLE, NAME[0]), ALIAS[0]);
        columnMap.put(new ColumnName(CATALOG, TABLE, NAME[1]), ALIAS[1]);
        Map<String, ColumnType> typemap = new LinkedHashMap();
        typemap.put(CATALOG + "." + TABLE + "." + NAME[0], TYPE[0]);
        typemap.put(CATALOG + "." + TABLE + "." + NAME[1], TYPE[1]);
        Map<ColumnName, ColumnType> typemapColumnName = new LinkedHashMap<>();
        typemapColumnName.put(new ColumnName(CATALOG, TABLE, NAME[0]), TYPE[0]);
        typemapColumnName.put(new ColumnName(CATALOG, TABLE, NAME[1]), TYPE[1]);

        Select select = new Select(Operations.SELECT_OPERATOR, columnMap, typemap, typemapColumnName);
        queryData.setSelect(select);

        return queryData;
    }

}
