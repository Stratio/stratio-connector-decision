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

package com.stratio.connector.decision.ftest.thread.actions;

import java.util.List;

import com.stratio.connector.decision.ftest.GenericDecisionTest;
import com.stratio.crossdata.common.data.Cell;
import com.stratio.crossdata.common.data.Row;
import com.stratio.crossdata.common.metadata.ColumnType;

/**
 * Created by jmgomez on 13/01/15.
 */
public class RowToInsertBigLong extends RowToInsertDefault {

	
	 @Override public Row getRowToInsert(Integer value, String text, List<ColumnType> typesToInsert, boolean
	            addIntegerChangeable, int integerChangeable) {
	        Row row = new Row();
	        for (ColumnType colType : typesToInsert) {

	            switch (colType.getDataType()) {
	            case BOOLEAN:
	                row.addCell(GenericDecisionTest.BOOLEAN_COLUMN, new Cell(true));
	                break;
	            case DOUBLE:
	                row.addCell(GenericDecisionTest.DOUBLE_COLUMN, new Cell(new Double(value + 0.5)));
	                break;
	            case FLOAT:
	                row.addCell(GenericDecisionTest.FLOAT_COLUMN, new Cell(new Float(value + 0.5)));
	                break;
	            case INT:
	                row.addCell(GenericDecisionTest.INTEGER_COLUMN, new Cell(value));
	                break;
	            case BIGINT:
	                row.addCell(GenericDecisionTest.LONG_COLUMN, new Cell(new Long(value + new Long(Long.MAX_VALUE / 2))));


	                break;
	            case VARCHAR:
	            case TEXT:
	                row.addCell(GenericDecisionTest.STRING_COLUMN, new Cell(text));
	                break;

	            }
	            if (addIntegerChangeable) {
	                row.addCell(GenericDecisionTest.INTEGER_CHANGEABLE_COLUMN, new Cell(integerChangeable));
	            }
	        }
	        return row;
	    }
	 
    protected Cell getLongCell(Integer value) {
        return new Cell(getBigLong(value));
    }

    public Long getBigLong(Integer value){
        return  new Long(value + new Long(Long.MAX_VALUE / 2));
    }
}
