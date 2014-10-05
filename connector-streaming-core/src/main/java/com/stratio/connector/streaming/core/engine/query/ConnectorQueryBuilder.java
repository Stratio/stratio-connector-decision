package com.stratio.connector.streaming.core.engine.query;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stratio.connector.streaming.core.engine.query.util.StreamUtil;
import com.stratio.meta.common.exceptions.ExecutionException;
import com.stratio.meta.common.exceptions.UnsupportedException;
import com.stratio.meta.common.logicalplan.Filter;
import com.stratio.meta.common.logicalplan.Select;
import com.stratio.meta.common.statements.structures.relationships.Operator;
import com.stratio.meta.common.statements.structures.relationships.Relation;
import com.stratio.meta.common.statements.structures.window.TimeUnit;
import com.stratio.meta.common.statements.structures.window.Window;
import com.stratio.meta.common.statements.structures.window.WindowType;
import com.stratio.meta2.common.data.ColumnName;
import com.stratio.meta2.common.statements.structures.selectors.ColumnSelector;
import com.stratio.meta2.common.statements.structures.selectors.Selector;
import com.stratio.meta2.common.statements.structures.selectors.StringSelector;

/**
 * Created by jmgomez on 30/09/14.
 */
public class ConnectorQueryBuilder {

    /**
     * The log.
     */
    final Logger logger = LoggerFactory.getLogger(this.getClass());

    private ConnectorQueryData queryData;
    private StringBuilder querySb = new StringBuilder();
    private String streamName;
    private String outgoing;

    public ConnectorQueryBuilder(ConnectorQueryData queryData) {
        this.queryData = queryData;
        // TODO String metaQueryId = queryData.getSelect().getQueryID();
        String metaQueryId = queryData.getQueryId();
        streamName = StreamUtil.createStreamName(queryData.getProjection());
        outgoing = StreamUtil.createOutgoingName(streamName, metaQueryId);
    }

    public String createQuery() throws ExecutionException, UnsupportedException {

        // #EXECUTION-QUERY => <input> <output> [<projection>]. See https://docs.wso2.com/display/CEP300/Language+Model

        createInputQuery();
        createProjection();// TODO check select
        createOutputQuery();

        return querySb.toString();
    }

    /**
     * 
     */
    private void createOutputQuery() {
        querySb.append(" insert into ");
        querySb.append(outgoing);
    }

    /**
     * @throws ExecutionException
     * 
     */
    private void createProjection() throws ExecutionException {

        List<String> ids = new ArrayList<>();
        Select selectionClause = queryData.getSelect();
        Map<ColumnName, String> aliasMapping = selectionClause.getColumnMap();
        Set<ColumnName> columnMetadataList = aliasMapping.keySet();

        // Retrieving the fields
        if (columnMetadataList == null || columnMetadataList.isEmpty()) {
            String message = "The query has to retrieve data";
            logger.error(message);
            throw new ExecutionException(message);
        } else {

            for (ColumnName columnName : columnMetadataList) {
                ids.add(columnName.getQualifiedName());
            }

        }

        querySb.append(" select ");

        // Retrieving the alias

        int numFields = ids.size();
        int i = 0;
        for (ColumnName id : aliasMapping.keySet()) {
            querySb.append(id.getName());//.append(" as ").append(aliasMapping.get(id));
            if (++i < numFields)
                querySb.append(",");
        }

    }

    /**
     * @param queryData
     * @return
     * @throws UnsupportedException
     */
    private void createInputQuery() throws UnsupportedException {
        querySb.append("from ");
        createStreamsQuery();
    }

    /**
     * @param queryData
     * @return
     * @throws UnsupportedException
     */
    private void createStreamsQuery() throws UnsupportedException {
        // only one logicalWorkflow so always
        createStreamQuery();
        createWindowQuery();
    }

    /**
     * @throws UnsupportedException
     * 
     */
    private void createWindowQuery() throws UnsupportedException {
        // TODO test if(queryData.hasWindow()) in createStreamsQuery
        Window window = new Window(WindowType.NUM_ROWS);
        window.setTimeWindow(5, TimeUnit.SECONDS);
        if (window != null) {
            if (window.getType() == WindowType.TEMPORAL) {
                querySb.append("#window.timeBatch( ").append(window.getDurationInMilliseconds())
                                .append(" milliseconds)");
            } else if (window.getType() == WindowType.NUM_ROWS) {
                // TODO window.getDuration()
                querySb.append("#window.lengthBatch(").append(String.valueOf(7)).append(")");
            } else
                throw new UnsupportedException("Window " + window.getType().toString() + " is not supported");
        }

    }

    /**
     * @throws UnsupportedException
     * 
     */
    private void createStreamQuery() throws UnsupportedException {
        querySb.append(streamName);
        if (queryData.hasFilterList()) {
            createConditionList();
        }

    }

    /**
     * @throws UnsupportedException
     * 
     */
    private void createConditionList() throws UnsupportedException {

        querySb.append("[");
        Iterator<Filter> filterIter = queryData.getFilter().iterator();
        while (filterIter.hasNext()) {
            Relation rel = filterIter.next().getRelation();

            querySb.append(getFieldName(rel.getLeftTerm())).append(" ").append(getSiddhiOperator(rel.getOperator()))
                            .append(" ");
            if (rel.getRightTerm() instanceof StringSelector) {
                querySb.append("'").append(((StringSelector) rel.getRightTerm()).getValue()).append("'");
            } else {
                switch (rel.getRightTerm().getType()) {
                case BOOLEAN:
                case INTEGER:
                case FLOATING_POINT:
                    querySb.append(rel.getRightTerm().toString());
                    break;
                case COLUMN:
                    querySb.append(getFieldName(rel.getRightTerm()));
                    break;
                case FUNCTION:
                case RELATION:
                case ASTERISK:
                default:
                    throw new UnsupportedException("Type " + rel.getRightTerm().getType() + "unsupported");
                }
            }

            if (filterIter.hasNext()) {
                querySb.append(" and ");
            }
        }
        querySb.append("]");

    }

    private static String getFieldName(Selector selector) throws UnsupportedException {
        String field = null;
        if (selector instanceof ColumnSelector) {
            ColumnSelector columnSelector = (ColumnSelector) selector;
            field = columnSelector.getName().getName();
        } else
            // TODO support right column selector
            throw new UnsupportedException("Left selector must be a columnSelector in filters");
        return field;
    }

    private static String getSiddhiOperator(Operator operator) throws UnsupportedException {

        String siddhiOperator;
        switch (operator) {

        case BETWEEN:
            throw new UnsupportedException("Not yet supported");
        case DISTINCT:
            siddhiOperator = "!=";
            break;
        case EQ:
            siddhiOperator = "==";
            break;
        case GET:
        case GT:
        case LET:
        case LT:
            siddhiOperator = operator.toString();
            break;

        default:
            throw new UnsupportedException("Operator " + operator.toString() + "is not supported");

        }

        return siddhiOperator;
    }

}
