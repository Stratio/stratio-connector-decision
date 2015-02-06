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

package com.stratio.connector.streaming.bean;

import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stratio.connector.commons.metadata.TableMetadataBuilder;
import com.stratio.connector.commons.test.util.LogicalWorkFlowCreator;
import com.stratio.crossdata.common.data.Cell;
import com.stratio.crossdata.common.data.ClusterName;
import com.stratio.crossdata.common.data.Row;
import com.stratio.crossdata.common.metadata.ColumnType;
import com.stratio.crossdata.common.metadata.TableMetadata;

/**
 * Created by jmgomez on 26/01/15.
 */
public class StreamingBean implements Serializable {

    private  static final Random random = new Random(System.currentTimeMillis());
    public static String TABLE_NAME = StreamingBean.class.getSimpleName()+Math.abs(random.nextInt());
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    public static void recalculateTableName(){
        TABLE_NAME = StreamingBean.class.getSimpleName()+Math.abs(random.nextInt());
    }


    public Integer id;

    public Integer age;

    public String name;

    public String favoritebookparagraph;

    public Long money;

    public Boolean married;

    public Float kilometer;

    public Double loan;

    public static final String AGE = "age";

    public static final String NAME = "name";

    public static final String FAVORITE_BOOK_PARAGRAPH = "favoritebookparagraph";

    public static final String MONEY = "money";

    public static final String MARRIED = "married";

    public static final String KILOMETER = "kilometer";

    public static final String LOAN = "loan";

    public static final String ID = "id";

    static Map<String,ColumnType> columnType = new HashMap<>();



    static {
        columnType.put(ID,ColumnType.INT);
        columnType.put(AGE,ColumnType.INT);
        columnType.put(NAME,ColumnType.VARCHAR);
        columnType.put(FAVORITE_BOOK_PARAGRAPH,ColumnType.TEXT);
        columnType.put(MONEY,ColumnType.BIGINT);
        columnType.put(MARRIED,ColumnType.BOOLEAN);
        columnType.put(KILOMETER,ColumnType.FLOAT);
        columnType.put(LOAN,ColumnType.DOUBLE);
    }

    public StreamingBean(Integer id,Integer age,String name, String favoriteBookParagraph,
            Long money, Boolean married, Float kilometer, Double loan){
        this.id = id;
        this.age = age;
        this.name = name;
        this.favoritebookparagraph = favoriteBookParagraph;
        this.money = money;
        this.married = married;
        this.kilometer = kilometer;
        this.loan = loan;

    }


    public Row toRow(){
        Row row = new Row();
        try {
        for (Map.Entry<String, ColumnType> cell : columnType.entrySet()) {
                row.addCell(cell.getKey(),new Cell(this.getClass().getField(cell.getKey()).get(this)));
        }

        } catch (NoSuchFieldException| IllegalAccessException e) {
            logger.error("Fail turn "+this.getClass().getName()+ " into Row."+e);
            throw new RuntimeException(e);
        }
        return row;
    }

    public static LogicalWorkFlowCreator getSelectAllWF(String catalog, ClusterName clusterName) {
        LogicalWorkFlowCreator lwCreator = new LogicalWorkFlowCreator(catalog,TABLE_NAME,
                clusterName);

        LinkedList<LogicalWorkFlowCreator.ConnectorField> selectColumns = new LinkedList<>();

        for (Map.Entry<String,ColumnType> columns: columnType.entrySet()) {
            selectColumns.add(lwCreator.createConnectorField(columns.getKey(), columns.getKey(),columns.getValue()));
        }
        lwCreator.addSelect(selectColumns);

        return lwCreator;
    }

    public static TableMetadata getTableMetadata(String catalog, ClusterName clusterName) {
        TableMetadataBuilder tableMetadataBuilder = new TableMetadataBuilder(catalog, TABLE_NAME, clusterName.getName
                ());
        for (Map.Entry<String,ColumnType> columns: columnType.entrySet()) {
            tableMetadataBuilder.addColumn(columns.getKey(),columns.getValue());
        }
        tableMetadataBuilder.withClusterKey(ID);

        return tableMetadataBuilder.build(true);
    }
}
