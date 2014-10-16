/*
 * Licensed to STRATIO (C) under one or more contributor license agreements.
 *  See the NOTICE file distributed with this work for additional information
 *  regarding copyright ownership. The STRATIO (C) licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License. You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied. See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package com.stratio.connector.streaming.core.engine.query.util;

import java.util.ArrayList;
import java.util.List;

import com.stratio.connector.streaming.core.engine.query.ConnectorQueryData;
import com.stratio.meta.common.connector.IResultHandler;
import com.stratio.meta.common.data.ResultSet;
import com.stratio.meta.common.data.Row;
import com.stratio.meta.common.exceptions.UnsupportedException;
import com.stratio.meta.common.logicalplan.Project;
import com.stratio.meta.common.logicalplan.Select;
import com.stratio.meta.common.metadata.structures.ColumnMetadata;
import com.stratio.meta.common.result.QueryResult;
import com.stratio.meta2.common.data.ColumnName;
import com.stratio.meta2.common.metadata.ColumnType;

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



    public void send(){
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
            String field = colName.getName();
            ColumnType colType = select.getTypeMap().get(colName.getQualifiedName());
            colType = updateColumnType(colType);
            ColumnMetadata columnMetadata = new ColumnMetadata(projection.getTableName().getName(), field, colType);
            columnMetadata.setColumnAlias(select.getColumnMap().get(colName));
            columnsMetadata.add(columnMetadata);
        }

    }


    private ColumnType updateColumnType(ColumnType colType) throws UnsupportedException {
        switch (colType) {

        case BIGINT:
            colType = ColumnType.DOUBLE;
            break;
        case BOOLEAN:
            colType = ColumnType.BOOLEAN;
            break;
        case DOUBLE:
            colType = ColumnType.DOUBLE;
            break;
        case FLOAT:
            colType = ColumnType.DOUBLE;
            break;
        case INT:
            colType = ColumnType.DOUBLE;
            break;
        case TEXT:
        case VARCHAR:
            colType = ColumnType.VARCHAR;
            break;
        default:
            throw new UnsupportedException("Column type " + colType.name() + " not supported in Streaming");
        }
        return colType;
    }


}
