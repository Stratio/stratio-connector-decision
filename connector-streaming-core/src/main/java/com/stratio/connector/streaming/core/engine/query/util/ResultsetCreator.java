/*
 * Licensed to STRATIO (C) under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information
 * regarding copyright ownership. The STRATIO (C) licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.stratio.connector.streaming.core.engine.query.util;

import java.util.ArrayList;
import java.util.List;

import com.stratio.connector.streaming.core.engine.query.ConnectorQueryData;
import com.stratio.crossdata.common.connector.IResultHandler;
import com.stratio.crossdata.common.data.ColumnName;
import com.stratio.crossdata.common.data.ResultSet;
import com.stratio.crossdata.common.data.Row;
import com.stratio.crossdata.common.exceptions.UnsupportedException;
import com.stratio.crossdata.common.logicalplan.Project;
import com.stratio.crossdata.common.logicalplan.Select;
import com.stratio.crossdata.common.metadata.ColumnType;
import com.stratio.crossdata.common.metadata.structures.ColumnMetadata;
import com.stratio.crossdata.common.result.QueryResult;

/**
 * Created by jmgomez on 16/10/14.
 */
public class ResultsetCreator {

    private List<ColumnMetadata> columnsMetadata;
    private ConnectorQueryData queryData;
    private IResultHandler resultHandler;
    private QueryResult queryResult;

    public ResultsetCreator(ConnectorQueryData queryData) throws UnsupportedException {
        this.queryData = queryData;
        createColumnMetadata();
    }

    public void setResultHandler(IResultHandler resultHandler) {
        this.resultHandler = resultHandler;
    }

    public void send() {
        resultHandler.processResult(queryResult);
    }

    public ResultsetCreator createResultSet(List<Row> rows) {
        ResultSet resultSet = new ResultSet();
        resultSet.setColumnMetadata(this.columnsMetadata);
        resultSet.setRows(rows);
        queryResult = QueryResult.createQueryResult(resultSet);
        queryResult.setQueryId(queryData.getQueryId());

        return this;

    }

    /**
     * @throws com.stratio.meta.common.exceptions.UnsupportedException
     */
    private void createColumnMetadata() throws UnsupportedException {
        columnsMetadata = new ArrayList<>();
        Select select = queryData.getSelect();
        Project projection = queryData.getProjection();

        for (ColumnName colName : select.getColumnMap().keySet()) {

            ColumnType colType = select.getTypeMap().get(colName.getQualifiedName());
            colType = updateColumnType(colType);
            ColumnMetadata columnMetadata = new ColumnMetadata(projection.getTableName().getQualifiedName(),
                    colName.getQualifiedName(), colType);
            columnMetadata.setColumnAlias(select.getColumnMap().get(colName));
            columnsMetadata.add(columnMetadata);
        }

    }

    private ColumnType updateColumnType(ColumnType colType) throws UnsupportedException {
        ColumnType columnReturn;
        switch (colType) {

        case BIGINT:
            columnReturn = ColumnType.DOUBLE;
            break;
        case BOOLEAN:
            columnReturn = ColumnType.BOOLEAN;
            break;
        case DOUBLE:
            columnReturn = ColumnType.DOUBLE;
            break;
        case FLOAT:
            columnReturn = ColumnType.DOUBLE;
            break;
        case INT:
            columnReturn = ColumnType.DOUBLE;
            break;
        case TEXT:
        case VARCHAR:
            columnReturn = ColumnType.VARCHAR;
            break;
        default:
            throw new UnsupportedException("Column type " + colType.name() + " not supported in Streaming");
        }
        return columnReturn;
    }

}
