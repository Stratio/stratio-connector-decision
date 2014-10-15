/**
 * Copyright (C) 2014 Stratio (http://stratio.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.stratio.connector.streaming.core.engine.query.queryExecutor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import com.stratio.connector.streaming.core.engine.query.ConnectorQueryData;
import com.stratio.meta.common.connector.IResultHandler;
import com.stratio.meta.common.connector.Operations;
import com.stratio.meta.common.data.Row;
import com.stratio.meta.common.logicalplan.Project;
import com.stratio.meta.common.logicalplan.Select;
import com.stratio.meta.common.metadata.structures.ColumnMetadata;
import com.stratio.meta2.common.data.ClusterName;
import com.stratio.meta2.common.data.ColumnName;
import com.stratio.meta2.common.data.QualifiedNames;
import com.stratio.meta2.common.data.TableName;
import com.stratio.meta2.common.metadata.ColumnType;

/**
 * @author david
 *
 */
public class ConnectorQueryExecutorTest {

    private static final String CATALOG_NAME = "CATALOG_NAME";
    private static final String TABLE_NAME = "TABLE_NAME";
    private static final String CLUSTER_NAME = "CLUSTER_NAME";
    private static final String QUERY_ID = "QUERY_ID";
    private static final String COLUMN_1 = "COLUMN_1";
    private static final String ALIAS_COL1 = "ALIAS_1";
    private static final String COLUMN_2 = "COLUMN_2";
    private static final String ALIAS_COL2 = "ALIAS_2";

    @Test
    public void testSetColumnMetadata() throws Exception {

        ConnectorQueryExecutor queryExecutor = new ConnectorQueryExecutor(createQueryData(), mock(IResultHandler.class)) {
            @Override
            protected void processMessage(Row row) {
            }
        };

        assertNotNull(queryExecutor.columnsMetadata);
        assertEquals(queryExecutor.columnsMetadata.size(), 2);

        ColumnMetadata columnMetadata = queryExecutor.columnsMetadata.get(0);
        ColumnMetadata columnMetadata2 = queryExecutor.columnsMetadata.get(1);

        boolean existFirstColumn = existFirstColumn(columnMetadata, columnMetadata2);
        boolean existSecondColumn = existSecondColumn(columnMetadata, columnMetadata2);

        assertTrue(existFirstColumn);
        assertTrue(existSecondColumn);

    }

    /**
     * @param columnMetadata
     * @param columnMetadata2
     * @return
     */
    private boolean existSecondColumn(ColumnMetadata columnMetadata, ColumnMetadata columnMetadata2) {
        boolean exist = false;
        ColumnMetadata colMetadata = (columnMetadata.getColumnName().equals(COLUMN_2)) ? columnMetadata
                        : columnMetadata2;
        exist = colMetadata.getColumnName().equals(COLUMN_2) && colMetadata.getTableName().equals(TABLE_NAME);
        assertEquals("verify alias", colMetadata.getColumnAlias(), ALIAS_COL2);
        // TODO updateColumn?? assertEquals("verify type",colMetadata.getType(), ColumnType.BIGINT);
        return exist;
    }

    /**
     * @param columnMetadata
     * @param columnMetadata2
     * @return
     */
    private boolean existFirstColumn(ColumnMetadata columnMetadata, ColumnMetadata columnMetadata2) {
        boolean exist = false;
        ColumnMetadata colMetadata = (columnMetadata.getColumnName().equals(COLUMN_1)) ? columnMetadata
                        : columnMetadata2;
        exist = colMetadata.getColumnName().equals(COLUMN_1) && colMetadata.getTableName().equals(TABLE_NAME);
        assertEquals("verify alias", colMetadata.getColumnAlias(), ALIAS_COL1);
        // TODO updateColumn?? assertEquals("verify type",colMetadata.getType(), ColumnType.BIGINT);
        return exist;
    }

    private ConnectorQueryData createQueryData() {
        ConnectorQueryData queryData = new ConnectorQueryData();
        Project projection = new Project(Operations.PROJECT, new TableName(CATALOG_NAME, TABLE_NAME), new ClusterName(
                        CLUSTER_NAME));
        Map<ColumnName, String> columnMap = new HashMap<ColumnName, String>();
        TableName tableName = new TableName(CATALOG_NAME, TABLE_NAME);
        columnMap.put(new ColumnName(tableName, COLUMN_1), ALIAS_COL1);
        columnMap.put(new ColumnName(tableName, COLUMN_2), ALIAS_COL2);
        Map<String, ColumnType> typeMap = new HashMap<String, ColumnType>();
        typeMap.put(QualifiedNames.getColumnQualifiedName(CATALOG_NAME, TABLE_NAME, COLUMN_1), ColumnType.BIGINT);
        typeMap.put(QualifiedNames.getColumnQualifiedName(CATALOG_NAME, TABLE_NAME, COLUMN_2), ColumnType.VARCHAR);
        Select select = new Select(Operations.SELECT_WINDOW, columnMap, typeMap);
        queryData.setProjection(projection);
        queryData.setSelect(select);
        queryData.setQueryId(QUERY_ID);
        return queryData;
    }
}
