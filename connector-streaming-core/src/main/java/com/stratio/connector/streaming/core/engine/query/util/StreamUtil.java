package com.stratio.connector.streaming.core.engine.query.util;

import com.stratio.meta.common.logicalplan.Project;

/**
 * Created by jmgomez on 1/10/14.
 */
public class StreamUtil {

    public static String createStreamName(Project project){
        return project.getCatalogName() + "_" + project.getTableName().getName();
    }

    public static String createOutgoingName(String streamName, String metaQueryId){
        return streamName + "_" + metaQueryId.replace("-", "_");
    }
}
