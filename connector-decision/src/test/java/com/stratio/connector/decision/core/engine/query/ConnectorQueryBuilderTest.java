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
package com.stratio.connector.decision.core.engine.query;

import static junit.framework.TestCase.assertEquals;

import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import com.stratio.crossdata.common.data.ClusterName;
import com.stratio.crossdata.common.data.ColumnName;
import com.stratio.crossdata.common.data.TableName;
import com.stratio.crossdata.common.logicalplan.Filter;
import com.stratio.crossdata.common.logicalplan.Project;
import com.stratio.crossdata.common.logicalplan.Select;
import com.stratio.crossdata.common.metadata.ColumnType;
import com.stratio.crossdata.common.metadata.DataType;
import com.stratio.crossdata.common.metadata.Operations;
import com.stratio.crossdata.common.statements.structures.ColumnSelector;
import com.stratio.crossdata.common.statements.structures.Operator;
import com.stratio.crossdata.common.statements.structures.Relation;
import com.stratio.crossdata.common.statements.structures.Selector;
import com.stratio.crossdata.common.statements.structures.StringSelector;

/**
 * ConnectorQueryBuilder Tester.
 *
 */
public class ConnectorQueryBuilderTest {

    private static final String COLUMN_2 = "column2";
    private static final String ALIAS_COLUMN_2 = "alias_column2";
    private static final String VALUE_2 = "value_2";
    private static final String CATALOG = "catalog";
    private static final String TABLE = "table";
    private static final String COLUMN_1 = "column1";
    private static final String VALUE_1 = "value1";
    private static final String CLUSTER_NAME = "clusterName";
    private static final String QUERY_ID = "qID";
    private static final String ALIAS_COLUMN_1 = "alias_column1";
    ConnectorQueryBuilder connectorQueryBuilder;
    private String resultExpected = "from catalog_table[column1 == 'value1' and column2 != 'value_2'] select catalog_table.column1 as alias_column1,catalog_table.column2 as alias_column2 insert into catalog_table_qID";

    @Before
    public void before() throws Exception {
        connectorQueryBuilder = new ConnectorQueryBuilder();
    }

    /**
     * Method: createQuery()
     */
    @Test
    public void createQueryTest() throws Exception {
        ConnectorQueryData queryData = createQueryData();
        assertEquals("The query is correct", resultExpected, connectorQueryBuilder.createQuery(queryData));
    }

    private ConnectorQueryData createQueryData() {
        ConnectorQueryData connectorQueryData = new ConnectorQueryData();
        connectorQueryData.addFilter(createFilter(COLUMN_1, Operator.EQ, VALUE_1));
        connectorQueryData.addFilter(createFilter(COLUMN_2, Operator.NOT_EQ, VALUE_2));
        connectorQueryData.setProjection(createProject());
        connectorQueryData.setQueryId(QUERY_ID);
        connectorQueryData.setSelect(createSelect());
        return connectorQueryData;
    }

    private Select createSelect() {
        Map<Selector, String> columMap = createColumnMap();
        Map<String, ColumnType> typeMap = createTypeMap();
        Map<Selector, ColumnType> typeMapColumnName = createTypeMapColumnName();
        return new Select(new HashSet<Operations>(Arrays.asList(Operations.SELECT_OPERATOR)), columMap, typeMap, typeMapColumnName);
    }

    private Map<Selector, ColumnType> createTypeMapColumnName() {
        Map<Selector, ColumnType> typeMap = new LinkedHashMap<>();
        typeMap.put(new ColumnSelector(new ColumnName(CATALOG, TABLE, COLUMN_1)), new ColumnType(DataType.DOUBLE));
        typeMap.put(new ColumnSelector(new ColumnName(CATALOG, TABLE, COLUMN_2)), new ColumnType(DataType.INT));
        return typeMap;
    }

    private Map<String, ColumnType> createTypeMap() {
        Map<String, ColumnType> typeMap = new LinkedHashMap<>();
        typeMap.put(COLUMN_1, new ColumnType(DataType.DOUBLE));
        typeMap.put(COLUMN_2, new ColumnType(DataType.INT));
        return typeMap;
    }

    private Map<Selector, String> createColumnMap() {
        Map<Selector, String> columMap = new LinkedHashMap();
        columMap.put(new ColumnSelector(new ColumnName(CATALOG, TABLE, COLUMN_1)), ALIAS_COLUMN_1);
        columMap.put(new ColumnSelector(new ColumnName(CATALOG, TABLE, COLUMN_2)), ALIAS_COLUMN_2);
        return columMap;
    }

    private Project createProject() {
        return new Project(new HashSet<Operations>(Arrays.asList(Operations.PROJECT)), new TableName(CATALOG, TABLE), new ClusterName(CLUSTER_NAME));
    }

    private Filter createFilter(String column, Operator operator, String value) {
        Selector leftSelector = new ColumnSelector(new ColumnName(CATALOG, TABLE, column));
        Selector rightSelector = new StringSelector(value);
        Relation relation = new Relation(leftSelector, operator, rightSelector);
        return new Filter(new HashSet<Operations>(Arrays.asList(Operations.FILTER_FUNCTION_EQ)), relation);
    }

}
