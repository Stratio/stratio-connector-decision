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
package com.stratio.connector.streaming.ftest.functionalInsert;

import org.junit.Test;

import com.stratio.connector.streaming.ftest.GenericStreamingTest;
import com.stratio.meta.common.exceptions.ExecutionException;
import com.stratio.meta.common.exceptions.UnsupportedException;
import com.stratio.meta2.common.data.ClusterName;

/**
 * Created by jmgomez on 16/07/14.
 */
public class SimpleInsertTest extends GenericStreamingTest {

    @Test
    public void testInsertInt() throws UnsupportedException, ExecutionException {
        ClusterName clusterName = getClusterName();
        System.out.println("*********************************** INIT FUNCTIONAL TEST testInsertInt "
                        + clusterName.getName() + " ***********************************");
        /*
         * Object value = new Integer(25); insertRow(clusterName, value4, ColumnType.FLOAT, VALUE_1, true);
         * 
         * ResultSet resultIterator = createResultSetTyped(clusterName, ColumnType.FLOAT);
         * assertEquals("It has only one result", 1, resultIterator.size()); for (Row recoveredRow : resultIterator) {
         * 
         * boolean typeCorrect = Float.class.getCanonicalName().equals(
         * recoveredRow.getCell(COLUMN_4).getValue().getClass().getCanonicalName()) ||
         * Double.class.getCanonicalName().equals( resultIterator.getColumnMetadata().get(3).getType().getDbClass()
         * .getCanonicalName()); assertTrue("The type is correct ", typeCorrect); assertEquals("The value is correct ",
         * new Double((float) value4), recoveredRow.getCell(COLUMN_4).getValue()); }
         */
    }

}
