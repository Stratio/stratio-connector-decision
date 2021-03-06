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

package com.stratio.connector.decision.core.engine.query.util;

import java.util.ArrayList;
import java.util.List;

import com.stratio.connector.decision.core.engine.query.ConnectorQueryData;
import com.stratio.connector.decision.core.exception.ExecutionValidationException;
import com.stratio.crossdata.common.connector.IResultHandler;
import com.stratio.crossdata.common.data.ColumnName;
import com.stratio.crossdata.common.data.ResultSet;
import com.stratio.crossdata.common.data.Row;
import com.stratio.crossdata.common.logicalplan.Select;
import com.stratio.crossdata.common.metadata.ColumnMetadata;
import com.stratio.crossdata.common.metadata.ColumnType;
import com.stratio.crossdata.common.result.QueryResult;
import com.stratio.crossdata.common.statements.structures.Selector;

/**
 * This class creates the resultset.
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
     * @param queryData
     *            the query data.
     * @throws ExecutionValidationException
     *             if a operation is not supported.
     */
    public ResultsetCreator(ConnectorQueryData queryData) throws ExecutionValidationException {
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
     * @param rows
     *            a row list.
     * @return a resultsetCreator.
     */
    public ResultsetCreator create(List<Row> rows) {
        ResultSet resultSet = new ResultSet();
        resultSet.setColumnMetadata(this.columnsMetadata);
        resultSet.setRows(rows);
        queryResult = QueryResult.createQueryResult(resultSet,0,false);
        queryResult.setQueryId(queryData.getQueryId());
        return this;

    }

    /**
     * Create the column metadata.
     *
     * @throws ExecutionValidationException
     *             if a type is not supported.
     */
    private void createColumnMetadata() {
        columnsMetadata = new ArrayList<>();
        Select select = queryData.getSelect();

        for (Selector colName : select.getColumnMap().keySet()) {

            ColumnType colType = select.getTypeMapFromColumnName().get(colName);
            ColumnName columnName = colName.getColumnName();
            columnName.setAlias(select.getColumnMap().get(colName));
            ColumnMetadata columnMetadata = new ColumnMetadata(columnName, null, colType);
            columnsMetadata.add(columnMetadata);
        }

    }

}
