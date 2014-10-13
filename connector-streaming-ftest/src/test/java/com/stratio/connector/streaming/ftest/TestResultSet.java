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

package com.stratio.connector.streaming.ftest;

import java.util.List;

import com.stratio.meta.common.connector.IResultHandler;
import com.stratio.meta.common.data.ResultSet;
import com.stratio.meta.common.data.Row;
import com.stratio.meta.common.exceptions.ExecutionException;
import com.stratio.meta.common.result.QueryResult;

/**
 * @author david
 *
 */

public class TestResultSet implements IResultHandler {

    List<QueryResult> resultSetList;

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
                for (Row row : queryRes.getResultSet().getRows()) {
                    resultSet.add(row);
                }
            }
        }
        return resultSet;
    }

}
