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

package com.stratio.connector.streaming.ftest.thread.actions;

import java.util.List;

import com.stratio.connector.streaming.ftest.GenericStreamingTest;
import com.stratio.crossdata.common.data.Cell;
import com.stratio.crossdata.common.data.Row;
import com.stratio.crossdata.common.metadata.ColumnType;

/**
 * Created by jmgomez on 13/01/15.
 */
public abstract class RowToInsert {
    /**
     * @param i
     * @return
     */
     public Row getRowToInsert(Integer value, String text, List<ColumnType> typesToInsert, boolean
            addIntegerChangeable, int integerChangeable) {
        Row row = new Row();
        for (ColumnType colType : typesToInsert) {

            switch (colType.getDataType()) {
            case BOOLEAN:
                row.addCell(GenericStreamingTest.BOOLEAN_COLUMN, getBooleanColumnCell());
                break;
            case DOUBLE:
                row.addCell(GenericStreamingTest.DOUBLE_COLUMN, getDoubleCell(value));
                break;
            case FLOAT:
                row.addCell(GenericStreamingTest.FLOAT_COLUMN, getFloatCell(value));
                break;
            case INT:
                row.addCell(GenericStreamingTest.INTEGER_COLUMN, getIntCell(value));
                break;
            case BIGINT:
                row.addCell(GenericStreamingTest.LONG_COLUMN, getLongCell(value));// + new Long(Long.MAX_VALUE / 2))));
                break;
            case VARCHAR:
            case TEXT:
                row.addCell(GenericStreamingTest.STRING_COLUMN, getTextCell(text));
                break;

            }
            if (addIntegerChangeable) {
                row.addCell(GenericStreamingTest.INTEGER_CHANGEABLE_COLUMN, getIntCell(integerChangeable));
            }
        }
        return row;
    }

    protected abstract Cell getTextCell(String text);

    protected abstract  Cell getLongCell(Integer value);

    protected abstract  Cell getIntCell(Integer value);

    protected abstract Cell getFloatCell(Integer value);

    protected abstract Cell getDoubleCell(Integer value);

    protected abstract Cell getBooleanColumnCell();


}
