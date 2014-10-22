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

import java.math.BigInteger;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stratio.crossdata.common.data.ColumnName;
import com.stratio.crossdata.common.data.TableName;
import com.stratio.crossdata.common.exceptions.UnsupportedException;
import com.stratio.crossdata.common.logicalplan.Project;
import com.stratio.crossdata.common.logicalplan.Select;
import com.stratio.crossdata.common.metadata.ColumnType;
import com.stratio.streaming.api.IStratioStreamingAPI;
import com.stratio.streaming.api.messaging.ColumnNameValue;
import com.stratio.streaming.commons.exceptions.StratioAPISecurityException;
import com.stratio.streaming.commons.exceptions.StratioEngineStatusException;

/**
 * Created by jmgomez on 1/10/14.
 */
public class StreamUtil {

    /**
     * The Log.
     */
    private final static Logger LOGGER = LoggerFactory.getLogger(StreamUtil.class);
    /**
     * a ramdom generator.
     */
    private static final Random RANDOM = new Random(System.currentTimeMillis());
    private static final String[] TEXT = { "A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N", "O",
                    "P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z", "a", "b", "c", "d", "e", "f", "g", "h", "i",
                    "j", "k", "l", "m", "n", "o", "p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z", "!", "_", "1",
                    "2", "3", "4", "5", "6", "7", "8", "9" };

    private StreamUtil() {
    }

    public static String createStreamName(Project project) {
        return createStreamName(project.getCatalogName(), project.getTableName().getName());
    }

    public static String createStreamName(TableName tableName) {
        return createStreamName(tableName.getCatalogName().getName(), tableName.getName());
    }

    private static String createStreamName(String catalog, String table) {
        return catalog + "_" + table;
    }

    public static String createOutgoingName(String streamName, String metaQueryId) {
        return streamName + "_" + metaQueryId.replace("-", "_");
    }

    public static void insertRandomData(IStratioStreamingAPI stratioStreamingAPI, String streamName, Select select)
                    throws UnsupportedException {
        try {

            List<ColumnNameValue> streamData = new LinkedList<>();
            for (ColumnName columnName : select.getColumnMap().keySet()) {
                String field = columnName.getName();
                ColumnType type = select.getTypeMap().get(columnName.getQualifiedName());
                streamData.add(new ColumnNameValue(field, getRandomValue(type)));
            }

            stratioStreamingAPI.insertData(streamName, streamData);
        } catch (StratioEngineStatusException | StratioAPISecurityException e) {
            LOGGER.error("Error inserting data in stream", e);
        }
    }

    private static Object getRandomValue(ColumnType type) throws UnsupportedException {
        Object randomObject;

        switch (type) {
        case INT:
            randomObject = RANDOM.nextInt();
            break;
        case BIGINT:
            randomObject = new BigInteger(500, RANDOM);
            break;
        case BOOLEAN:
            randomObject = RANDOM.nextBoolean();
            break;
        case DOUBLE:
            randomObject = RANDOM.nextDouble();
            break;
        case FLOAT:
            randomObject = RANDOM.nextFloat();
            break;

        case TEXT:
        case VARCHAR:
            randomObject = getRandonText();
            break;
        default:
            throw new UnsupportedException("Type " + type + " is not supported in streaming");

        }
        return randomObject;
    }

    private static Object getRandonText() {

        Object randomObject;

        randomObject = getRandonLetter() + getRandonLetter() + getRandonLetter() + getRandonLetter()
                        + getRandonLetter() + getRandonLetter() + getRandonLetter() + getRandonLetter()
                        + getRandonLetter() + getRandonLetter() + getRandonLetter() + getRandonLetter()
                        + getRandonLetter() + getRandonLetter();

        return randomObject;
    }

    private static String getRandonLetter() {
        return TEXT[Math.abs(RANDOM.nextInt() % TEXT.length)];
    }

}
