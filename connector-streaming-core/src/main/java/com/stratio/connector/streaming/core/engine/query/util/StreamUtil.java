package com.stratio.connector.streaming.core.engine.query.util;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stratio.meta.common.exceptions.UnsupportedException;
import com.stratio.meta.common.logicalplan.Project;
import com.stratio.meta.common.logicalplan.Select;
import com.stratio.meta2.common.data.ColumnName;
import com.stratio.meta2.common.data.TableName;
import com.stratio.meta2.common.metadata.ColumnType;
import com.stratio.streaming.api.IStratioStreamingAPI;
import com.stratio.streaming.api.messaging.ColumnNameValue;
import com.stratio.streaming.commons.exceptions.StratioAPISecurityException;
import com.stratio.streaming.commons.exceptions.StratioEngineStatusException;

/**
 * Created by jmgomez on 1/10/14.
 */
public class StreamUtil {

    private static Random  random=  new Random(System.currentTimeMillis());

    /**
     * The Log.
     */
    private final static Logger logger = LoggerFactory.getLogger(StreamUtil.class);

    private static int numItem = 0;

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

    public static void insertRandomData(IStratioStreamingAPI stratioStreamingAPI, String streamName,Select select)
            throws UnsupportedException {
        try {
       double randomDouble = Math.random() * 100;
            int randomInt = (int) (randomDouble * Math.random() * 2);
            StringBuilder sb = new StringBuilder(String.valueOf(randomDouble));
            sb.append(randomInt);
            String str = convertRandomNumberToString(sb.toString()) + "___" + numItem;
            numItem++;
            if (numItem == 20) {
                numItem = 0;
            }

            List<ColumnNameValue> streamData = new LinkedList<>();
            for (ColumnName columnName: select.getColumnMap().keySet()){
                String field = columnName.getName();
                ColumnType type = select.getTypeMap().get(columnName.getQualifiedName());
                streamData.add(new ColumnNameValue(field,getRamdomValue(type)));
            }




                stratioStreamingAPI.insertData(streamName, streamData);
        } catch (StratioEngineStatusException | StratioAPISecurityException e) {
            logger.error("Error inserting data in stream", e);
        }
    }



    private static Object getRamdomValue(ColumnType type) throws UnsupportedException {
        Object ramdomObject = null;

        switch (type){
            case INT:ramdomObject = random.nextInt(); break;
            case BIGINT: ramdomObject = new BigInteger(500,random); break;
            case BOOLEAN: ramdomObject = random.nextBoolean(); break;
            case DOUBLE: ramdomObject = random.nextDouble(); break;
            case FLOAT: break;

            case TEXT:
            case VARCHAR:
                    break;
        default: throw new UnsupportedException("Type "+type+" is not supported in streaming");


        }
        return ramdomObject;
    }

    public static String convertRandomNumberToString(String str) {
        return str.replace('0', 'o').replace('1', 'i').replace('2', 'u').replace('3', 'e')
                .replace('4', 'a').replace('5', 'b').replace('6', 'c').replace('7', 'd').replace('8', 'f')
                .replace('9', 'g').replace('.', 'm');
    }
}
