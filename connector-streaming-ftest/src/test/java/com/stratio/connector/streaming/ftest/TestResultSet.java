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

package com.stratio.connector.streaming.ftest;

import java.util.ArrayList;
import java.util.List;

import com.stratio.crossdata.common.data.Row;
import com.stratio.crossdata.common.result.QueryResult;

/**
 * @author david
 *
 */

public class TestResultSet implements IResultHandler {

    List<QueryResult> resultSetList = new ArrayList<QueryResult>();

    @Override
    public void processException(String queryId, ExecutionException exception) {
        System.out.println(queryId + " " + exception.getMessage());
        exception.printStackTrace();
    }

    @Override
    public void processResult(QueryResult result) {
        resultSetList.add(result);
    }

    public List<QueryResult> getQueryResultList() {
        return resultSetList;
    }

    public ResultSet getResultSet(String queryId) {
        ResultSet resultSet = new ResultSet();
        for (QueryResult queryRes : resultSetList) {
            if (queryRes.getQueryId() == queryId) {
                // TODO verify the column metadata is equal
                for (Row row : queryRes.getResultSet().getRows()) {
                    resultSet.add(row);
                }
                resultSet.setColumnMetadata(queryRes.getResultSet().getColumnMetadata());
            }
        }

        return resultSet;
    }

}
