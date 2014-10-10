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
    private static  String[] text = {"A","B","C","D","E","F","G","H","I","J","K","L","M","N","O","P","Q","R","S","T",
            "U","V","W","X","Y","Z","a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s",
            "t","u","v","w","x","y","z"};


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
        Object ramdomObject;



        switch (type){
            case INT:ramdomObject = random.nextInt(); break;
            case BIGINT: ramdomObject = new BigInteger(500,random); break;
            case BOOLEAN: ramdomObject = random.nextBoolean(); break;
            case DOUBLE: ramdomObject = random.nextDouble(); break;
            case FLOAT:ramdomObject = random.nextFloat(); break;

            case TEXT:
            case VARCHAR:
                ramdomObject = getRamndonText();
                    break;
        default: throw new UnsupportedException("Type "+type+" is not supported in streaming");


        }
        return ramdomObject;
    }

    private static Object getRamndonText() {

        Object ramdomObject;

        ramdomObject = getRamdonLetter() + getRamdonLetter() + getRamdonLetter() +getRamdonLetter()+getRamdonLetter()
                +getRamdonLetter()+getRamdonLetter()+getRamdonLetter() + getRamdonLetter() + getRamdonLetter() +getRamdonLetter()+getRamdonLetter()
                +getRamdonLetter()+getRamdonLetter();

        return ramdomObject;
    }



    private static String getRamdonLetter() {
        return text[Math.abs(random.nextInt())%text.length];
    }

}
