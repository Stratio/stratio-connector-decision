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
import com.stratio.crossdata.common.metadata.ColumnMetadata;
import com.stratio.crossdata.common.metadata.ColumnType;
import com.stratio.crossdata.common.result.QueryResult;

/**
 * This class creates the resultset. Created by jmgomez on 16/10/14.
 */
public class ResultsetCreator {

    /**
     * The column metadata.
     */
    private List<ColumnMetadata> columnsMetadata;
    /**
     * The query data.
     */
    private ConnectorQueryData queryData;
    /**
     * The result handler.
     */
    private IResultHandler resultHandler;
    /**
     * The query result.
     */
    private QueryResult queryResult;

    /**
     * Constructor.
     *
     * @param queryData the query data.
     * @throws UnsupportedException if a operation is not supperted.
     */
    public ResultsetCreator(ConnectorQueryData queryData) throws UnsupportedException {
        this.queryData = queryData;
        createColumnMetadata();
    }

    /**
     * Set the result handler.
     *
     * @param resultHandler
     */
    public void setResultHandler(IResultHandler resultHandler) {
        this.resultHandler = resultHandler;
    }

    /**
     * Send the resultset.
     */
    public void send() {
        resultHandler.processResult(queryResult);
    }

    /**
     * Create a resultset.
     *
     * @param rows a row list.
     * @return a resultsetCreator.
     */
    public ResultsetCreator create(List<Row> rows) {
        ResultSet resultSet = new ResultSet();
        resultSet.setColumnMetadata(this.columnsMetadata);
        resultSet.setRows(rows);
        queryResult = QueryResult.createQueryResult(resultSet);
        queryResult.setQueryId(queryData.getQueryId());

        return this;

    }

    /**
     * Create the column metadata.
     *
     * @throws UnsupportedException if a type is not supported.
     */
    private void createColumnMetadata() throws UnsupportedException {
        columnsMetadata = new ArrayList<>();
        Select select = queryData.getSelect();
        Project projection = queryData.getProjection();

        for (ColumnName colName : select.getColumnMap().keySet()) {

            ColumnType colType = select.getTypeMapFromColumnName().get(colName);
            colName.setAlias(select.getColumnMap().get(colName));
            ColumnMetadata columnMetadata = new ColumnMetadata(colName, null, colType);
            columnsMetadata.add(columnMetadata);
        }

    }

}
