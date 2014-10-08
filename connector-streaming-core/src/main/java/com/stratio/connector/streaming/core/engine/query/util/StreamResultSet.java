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
package com.stratio.connector.streaming.core.engine.query.util;

import java.util.ArrayList;
import java.util.List;

import com.stratio.connector.streaming.core.engine.query.ConnectorQueryData;
import com.stratio.meta.common.data.ResultSet;
import com.stratio.meta.common.data.Row;
import com.stratio.meta.common.metadata.structures.ColumnMetadata;

/**
 * @author darroyo
 * 
 */
public class StreamResultSet {

    private ResultSet resultSet;
    private List<String> primaryKey;

    public StreamResultSet(ConnectorQueryData queryData) {
        resultSet = new ResultSet();
        // TODO create listPK
        primaryKey = new ArrayList<String>();
    }

    public void add(Row row) {
        // TODO check if there is duplicates
        if (primaryKey.isEmpty())
            resultSet.add(row);
        else {

        }
    }

    public void setColumnMetadata(List<ColumnMetadata> colMetadata) {
        // TODO private setColumnMetadata
        resultSet.setColumnMetadata(colMetadata);
    }

    public int size() {
        return resultSet.size();
    }

    public ResultSet getResultSet() {
        return resultSet;
    }

}
