package cn.wxn;

import lombok.extern.slf4j.Slf4j;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.*;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * 分析SELECT语句,获取该语句中涉及到的所有column的信息
 *
 * @author wxn
 * @since 2023/8/9
 */
@Slf4j
public class SQLSelectAnalyzer {
    Map<Integer, QueryRecord> globalQueryRecords = new HashMap<>();
    private int SEQUENCE = 1;
    private int QUERY_NUM = 1;

    private DatabaseMetaData metaData;

    private String catalog;

    private int sequence() {
        return SEQUENCE++;
    }

    private int queryNum() {
        return QUERY_NUM++;
    }


    public Map<Integer, QueryRecord> analyzeSelectBody(SelectBody selectBody) {
        analyzeSelectBodyInner(selectBody);
        linkQueryRecords(globalQueryRecords.get(1));
        return globalQueryRecords;
    }

    /**
     * 通过数据库连接查询元数据，补充select *中的内容
     *
     * @param selectBody
     * @param connection
     * @return
     */
    public Map<Integer, QueryRecord> analyzeSelectBody(SelectBody selectBody, Connection connection) throws SQLException {
        this.catalog = connection.getCatalog();
        this.metaData = connection.getMetaData();
        analyzeSelectBodyInner(selectBody);
        processStarColumn(globalQueryRecords.get(1));
        linkQueryRecords(globalQueryRecords.get(1));
        return globalQueryRecords;
    }

    private void processStarColumn(QueryRecord queryRecord) {
        List<QueryRecord> childQueries = queryRecord.getChildQueries();
        if (CollectionUtils.isNotEmpty(childQueries)) {
            for (QueryRecord childQuery : childQueries) {
                processStarColumn(childQuery);
            }
        }
        Set<ColumnRecord> selectColumns = queryRecord.getSelectColumns();
        Iterator<ColumnRecord> iterator = selectColumns.iterator();
        Set<ColumnRecord> tempColumns = new HashSet<>();
        while (iterator.hasNext()) {
            ColumnRecord selectColumn = iterator.next();
            if (selectColumn.getColumnName().equals("*")) {
                iterator.remove();
                if (selectColumn.isFinishAnalyse()) {
                    //1. 如果是finishAnalyse, 即最底层的select * from table，则直接替换
                    try {
                        List<String> columnNamesFromTableName = getColumnNamesFromTableName(selectColumn.getTableAlias());
                        Set<ColumnRecord> columnRecords = columnNamesFromTableName.stream().map(x -> {
                            ColumnRecord cr = new ColumnRecord();
                            cr.setColumnName(x);
                            cr.setColumnAlias(x);
                            cr.setTableAlias(selectColumn.getTableAlias());
                            cr.setFinishAnalyse(true);
                            return cr;
                        }).collect(Collectors.toSet());
                        tempColumns.addAll(columnRecords);
                    } catch (Exception e) {
                        log.error("通过数据库连接解析原始数据表{}中的*（全字段）时出错!", selectColumn.getTableAlias(), e);
                    }
                } else {
                    //2. 如果不是如果是finishAnalyse，则需要从子查询从获取信息
                    if (CollectionUtils.isEmpty(childQueries)) {
                        return;
                    }
                    List<QueryRecord> childQueryRecords = childQueries.stream().filter(qr -> {
                        if (StringUtils.isEmpty(selectColumn.getTableAlias())) {
                            //如果直接是select * 而没有表别名，则将from后的子查询所有column都纳入到*的范畴内
                            return qr.isSubQueryInFrom();
                        } else {
                            //如果跟着表别名select a.*，则只处理别名为a的子查询
                            return qr.isSubQueryInFrom() && StringUtils.isNotEmpty(qr.getQueryAlias()) && qr.getQueryAlias().equals(selectColumn.getTableAlias());
                        }
                    }).collect(Collectors.toList());
                    for (QueryRecord childQuery : childQueryRecords) {
                        Set<ColumnRecord> childQuerySelectColumns = childQuery.getSelectColumns();
                        for (ColumnRecord childColumn : childQuerySelectColumns) {
                            ColumnRecord columnRecord = new ColumnRecord();
                            columnRecord.setColumnName(childColumn.getColumnAlias());
                            columnRecord.setFinishAnalyse(false);
                            columnRecord.setColumnAlias(childColumn.getColumnAlias());
                            columnRecord.setTableAlias(childQuery.getQueryAlias());
                            tempColumns.add(columnRecord);
                        }
                    }
                }
            }
        }
        selectColumns.addAll(tempColumns);
    }

    private void linkQueryRecords(QueryRecord queryRecord) {
        List<QueryRecord> childQueries = queryRecord.getChildQueries().stream().filter(x -> !x.isSubQueryInSelect()).collect(Collectors.toList());
        if (CollectionUtils.isEmpty(childQueries)) {
            return;
        }
        //key: 输出的字段名, value：组成改输出字段名的输入字段
        Map<String, Set<ColumnRecord>> columnNameWithTableAliasChildQueryRecordMap = new HashMap<>();
        for (QueryRecord childQuery : childQueries) {
            for (ColumnRecord columnRecord : childQuery.getSelectColumns()) {
                String columnNameWithTable = childQuery.getQueryAlias() + "." + columnRecord.getColumnAlias();
                if (columnNameWithTableAliasChildQueryRecordMap.containsKey(columnNameWithTable)) {
                    columnNameWithTableAliasChildQueryRecordMap.get(columnNameWithTable).add(columnRecord);
                } else {
                    columnNameWithTableAliasChildQueryRecordMap.put(columnNameWithTable, new HashSet<>(Collections.singletonList(columnRecord)));
                }
            }
        }
        processSourceColumns(queryRecord.getSelectColumns(), columnNameWithTableAliasChildQueryRecordMap);
        processSourceColumns(queryRecord.getWhereColumns(), columnNameWithTableAliasChildQueryRecordMap);
        processSourceColumns(queryRecord.getOrderByColumns(), columnNameWithTableAliasChildQueryRecordMap);
        processSourceColumns(queryRecord.getGroupByColumns(), columnNameWithTableAliasChildQueryRecordMap);
        processSourceColumns(queryRecord.getHavingColumns(), columnNameWithTableAliasChildQueryRecordMap);
        processSourceColumns(queryRecord.getJoinOnColumns(), columnNameWithTableAliasChildQueryRecordMap);
        for (QueryRecord childQuery : childQueries) {
            linkQueryRecords(childQuery);
        }
    }

    private void processSourceColumns(Set<ColumnRecord> columnRecords, Map<String, Set<ColumnRecord>> columnNameWithTableAliasChildQueryRecordMap) {
        for (ColumnRecord columnRecord : columnRecords) {
            if (StringUtils.isEmpty(columnRecord.getTableAlias())) {
                continue;
            }
            if (columnRecord.isFinishAnalyse()) {
                continue;
            }
            Set<ColumnRecord> sourceColumnRecords = columnNameWithTableAliasChildQueryRecordMap.get(columnRecord.getTableAlias() + "." + columnRecord.getColumnName());
            if (CollectionUtils.isEmpty(sourceColumnRecords)) {
                log.error("处理失败的column:{}", columnRecord);
                continue;
            }
            columnRecord.getSourceColumns().addAll(sourceColumnRecords);
        }
    }

    private QueryRecord analyzeSelectBodyInner(SelectBody selectBody) {
        QueryRecord queryRecord = new QueryRecord();
        //SELECT语句处理两种情况 SetOperation和 PlainSelect
        //TODO: 暂不支持WITH语句
        if (selectBody instanceof SetOperationList) {
            int queryNum = queryNum();
            queryRecord.setQueryId(queryNum);
            for (SelectBody subSelectBody : ((SetOperationList) selectBody).getSelects()) {
                QueryRecord subQueryRecord = analyzeSelectBodyInner(subSelectBody);
                subQueryRecord.setParentQueryId(queryNum);
                String queryAlias = "SetOperation" + sequence();
                subQueryRecord.setQueryAlias(queryAlias);
                subQueryRecord.setSubQueryInFrom(true);
                queryRecord.getChildQueries().add(subQueryRecord);
                Set<ColumnRecord> selectColumns = queryRecord.getSelectColumns();
                for (ColumnRecord childSelectColumn: subQueryRecord.getSelectColumns()){
                    ColumnRecord columnRecord = SerializationUtils.clone(childSelectColumn);
                    columnRecord.setColumnName(childSelectColumn.getColumnAlias());
                    columnRecord.setTableAlias(queryAlias);
                    columnRecord.setFinishAnalyse(false);
                    selectColumns.add(columnRecord);
                }
            }
            globalQueryRecords.put(queryNum, queryRecord);
        } else if (selectBody instanceof PlainSelect) {
            queryRecord = analyzePlainSelect((PlainSelect) selectBody);
        } else {
            log.debug("暂不支持的SELECT语句类型：{},示例:{}", selectBody.getClass(), selectBody);
            int queryNum = queryNum();
            queryRecord.setQueryId(queryNum);
            globalQueryRecords.put(queryNum, queryRecord);
        }
        return queryRecord;
    }

    /**
     * @param plainSelect
     */
    private QueryRecord analyzePlainSelect(PlainSelect plainSelect) {
        QueryRecord queryRecord = new QueryRecord();
        int queryNum = queryNum();
        queryRecord.setQueryId(queryNum);
        globalQueryRecords.put(queryNum, queryRecord);

        FromItem fromItem = plainSelect.getFromItem();
        //没有FROM语句那么就不涉及到表字段
        if (fromItem == null) {
            return queryRecord;
        }
        //TODO: 如果是  FROM JOIN形式的，那么SELECT的表字段前面都应该有表别名，表明来自JOIN的哪部分，否则需要借助元数据判断，暂不处理
        if (fromItem.getAlias() != null) {
            fromItem.setAlias(new Alias(format(fromItem.getAlias().getName())));
        } else if (fromItem instanceof Table) {
            fromItem.setAlias(new Alias(format(((Table) fromItem).getName())));
        } else {
            log.debug("FROM ITEM 并非TABLE，但是缺少别名，自动补充");
            fromItem.setAlias(new Alias(format("FromAlias" + sequence())));
        }
        List<Join> joins = plainSelect.getJoins();
        boolean isJoin = CollectionUtils.isNotEmpty(joins);
        if (isJoin){
            for (Join join : joins) {
                //把Join语句当作FROM Item处理
                FromItem rightItem = join.getRightItem();
                if (rightItem.getAlias() == null) {
                    String alias = rightItem instanceof Table ? ((Table) rightItem).getName() : format("FromAlias" + sequence());
                    rightItem.setAlias(new Alias(alias));
                }
            }
        }
        //解析SELECT中涉及到的字段
        Set<ColumnRecord> selectColumns = new HashSet<>();
        List<SelectItem> selectColumn = plainSelect.getSelectItems();
        for (SelectItem selectItem : selectColumn) {
            //*当作普通字段来处理
            if (selectItem instanceof AllColumns) {
                ColumnRecord columnRecord = new ColumnRecord();
                columnRecord.setColumnName("*");
                columnRecord.setColumnAlias("*");
                columnRecord.setTableAlias(fromItem.getAlias().getName());
                selectColumns.add(columnRecord);
                if (isJoin){
                    for (Join join : joins) {
                        //把Join语句当作FROM Item处理
                        FromItem rightItem = join.getRightItem();
                        ColumnRecord cr = new ColumnRecord();
                        cr.setColumnName("*");
                        cr.setColumnAlias("*");
                        cr.setTableAlias(rightItem.getAlias().getName());
                        selectColumns.add(cr);
                    }
                }
                continue;
            }
            if (selectItem instanceof AllTableColumns) {
                ColumnRecord columnRecord = new ColumnRecord();
                columnRecord.setColumnName("*");
                columnRecord.setColumnAlias("*");
                columnRecord.setTableAlias(((AllTableColumns) selectItem).getTable().getName());
                selectColumns.add(columnRecord);
                continue;
            }
            SelectExpressionItem expressionItem = (SelectExpressionItem) selectItem;
            Expression expression = expressionItem.getExpression();
            //SelectItem中的SubSelect比较特殊，其查出来的字段直接作用于parent Query
            if (expression instanceof SubSelect) {
                QueryRecord subQueryRecord = analyzeSelectBodyInner(((SubSelect) expression).getSelectBody());
                subQueryRecord.setSubQueryInSelect(true);
                subQueryRecord.setParentQueryId(queryNum);
                queryRecord.getChildQueries().add(subQueryRecord);
                for (ColumnRecord c : subQueryRecord.getSelectColumns()) {
                    ColumnRecord pc = SerializationUtils.clone(c);
                    pc.getSourceColumns().add(c);
                    selectColumns.add(pc);
                }
            } else {
                List<ColumnRecord> columnRecords = analyzeExpression(queryNum, expression, expressionItem.getAlias());
                columnRecords.forEach(x -> {
                    if (x.getTableAlias() == null && !isJoin) {
                        x.setTableAlias(fromItem.getAlias().getName());
                    }
                });
                selectColumns.addAll(columnRecords);
            }
        }
        queryRecord.setSelectColumns(selectColumns);
        //解析WHERE中涉及到的字段
        Expression where = plainSelect.getWhere();
        if (where != null) {
            List<ColumnRecord> columnRecords = analyzeExpression(queryNum, where, null);
            columnRecords.forEach(x -> {
                if (x.getTableAlias() == null && !isJoin) {
                    x.setTableAlias(fromItem.getAlias().getName());
                }
            });
            queryRecord.getWhereColumns().addAll(columnRecords);
        }
        //解析ORDER BY中涉及到的字段
        List<OrderByElement> orderByElements = plainSelect.getOrderByElements();
        if (CollectionUtils.isNotEmpty(orderByElements)) {
            for (OrderByElement orderByElement : orderByElements) {
                List<ColumnRecord> columnRecords = analyzeExpression(queryNum, orderByElement.getExpression(), null);
                columnRecords.forEach(x -> {
                    if (x.getTableAlias() == null && !isJoin) {
                        x.setTableAlias(fromItem.getAlias().getName());
                    }
                });
                queryRecord.getOrderByColumns().addAll(columnRecords);
            }
        }
        //解析GROUP BY中涉及到的字段
        GroupByElement groupBy = plainSelect.getGroupBy();
        if (groupBy != null && groupBy.getGroupByExpressionList() != null) {
            for (Expression expression : groupBy.getGroupByExpressionList().getExpressions()) {
                List<ColumnRecord> columnRecords = analyzeExpression(queryNum, expression, null);
                columnRecords.forEach(x -> {
                    if (x.getTableAlias() == null && !isJoin) {
                        x.setTableAlias(fromItem.getAlias().getName());
                    }
                });
                queryRecord.getGroupByColumns().addAll(columnRecords);
            }
        }
        //解析HAVING中涉及到的字段
        Expression having = plainSelect.getHaving();
        if (having != null) {
            List<ColumnRecord> columnRecords = analyzeExpression(queryNum, having, null);
            columnRecords.forEach(x -> {
                if (x.getTableAlias() == null && !isJoin) {
                    x.setTableAlias(fromItem.getAlias().getName());
                }
            });
            queryRecord.getHavingColumns().addAll(columnRecords);
        }
        //解析 JOIN ON
        if (isJoin) {
            for (Join join : joins) {
                if (CollectionUtils.isNotEmpty(join.getOnExpressions())) {
                    for (Expression onExpression : join.getOnExpressions()) {
                        queryRecord.getJoinOnColumns().addAll(analyzeExpression(queryNum, onExpression, null));
                    }
                }
            }
        }
        //解析FROM语句 (此时的FROM之后的内容都跟着别名)
        analyzeFromItem(queryNum, fromItem, false);
        //解析JOIN语句
        if (isJoin) {
            for (Join join : joins) {
                //把Join语句当作FROM Item处理
                FromItem rightItem = join.getRightItem();
                if (rightItem.getAlias() == null) {
                    String alias = rightItem instanceof Table ? ((Table) rightItem).getName() : format("FromAlias" + sequence());
                    rightItem.setAlias(new Alias(alias));
                }
                analyzeFromItem(queryNum, join.getRightItem(), false);
            }
        }
        return queryRecord;
    }

    private void analyzeFromItem(int curQueryNum, FromItem fromItem, boolean isSubJoin) {
        String fromItemAliasName = format(fromItem.getAlias().getName());
        if (fromItem instanceof Table) {
            QueryRecord parentQueryRecord = globalQueryRecords.get(curQueryNum);
            if (isSubJoin) {
                ColumnRecord columnRecord = new ColumnRecord();
                columnRecord.setColumnName("*");
                columnRecord.setColumnAlias("*");
                columnRecord.setTableAlias(((Table) fromItem).getName());
                parentQueryRecord.getSelectColumns().add(columnRecord);
            } else {
                updateQueryRecordWithTableName(parentQueryRecord, ((Table) fromItem).getName(), fromItemAliasName);
            }
        } else if (fromItem instanceof SubSelect) {
            QueryRecord queryRecord = analyzeSelectBodyInner(((SubSelect) fromItem).getSelectBody());
            queryRecord.setQueryAlias(fromItemAliasName);
            queryRecord.setParentQueryId(curQueryNum);
            queryRecord.setSubQueryInFrom(true);
            QueryRecord parentQueryRecord = globalQueryRecords.get(curQueryNum);
            parentQueryRecord.getChildQueries().add(queryRecord);
            if (isSubJoin) {
                for (ColumnRecord c : queryRecord.getSelectColumns()) {
                    ColumnRecord columnRecord = new ColumnRecord();
                    columnRecord.setColumnName(c.getColumnAlias());
                    columnRecord.setColumnAlias(c.getColumnAlias());
                    columnRecord.setTableAlias(queryRecord.getQueryAlias());
                    parentQueryRecord.getSelectColumns().add(columnRecord);
                }
            }
        } else if (fromItem instanceof SubJoin) {
            FromItem leftItem = ((SubJoin) fromItem).getLeft();
            if (leftItem.getAlias() == null) {
                log.debug("FROM ITEM 为SubJoin，但是缺少别名，自动补充");
                leftItem.setAlias(new Alias("FromAlias" + sequence()));
            }
            QueryRecord queryRecord = new QueryRecord();
            int queryId = queryNum();
            queryRecord.setSubQueryInFrom(true);
            queryRecord.setParentQueryId(curQueryNum);
            queryRecord.setQueryId(queryId);
            globalQueryRecords.get(curQueryNum).getChildQueries().add(queryRecord);
            globalQueryRecords.put(queryId, queryRecord);
            analyzeFromItem(queryId, leftItem, true);
            for (Join join : ((SubJoin) fromItem).getJoinList()) {
                FromItem rightItem = join.getRightItem();
                if (rightItem.getAlias() == null) {
                    log.debug("FROM ITEM 为SubJoin，但是缺少别名，自动补充");
                    rightItem.setAlias(new Alias("FromAlias" + sequence()));
                }
                //把Join语句当作FROM Item处理
                analyzeFromItem(queryId, join.getRightItem(), true);
            }
            queryRecord.setQueryAlias(fromItemAliasName);
        } else {
            log.error("暂不支持的FROM ITEM类型：{},示例{}", fromItem.getClass(), fromItem);
        }
    }

    private void updateQueryRecordWithTableName(QueryRecord parentQueryRecord, String tableName, String fromItemAliasName) {
        updateQueryRecordWithTableName(parentQueryRecord.getSelectColumns(), tableName, fromItemAliasName);
        updateQueryRecordWithTableName(parentQueryRecord.getWhereColumns(), tableName, fromItemAliasName);
        updateQueryRecordWithTableName(parentQueryRecord.getOrderByColumns(), tableName, fromItemAliasName);
        updateQueryRecordWithTableName(parentQueryRecord.getGroupByColumns(), tableName, fromItemAliasName);
        updateQueryRecordWithTableName(parentQueryRecord.getHavingColumns(), tableName, fromItemAliasName);
        updateQueryRecordWithTableName(parentQueryRecord.getJoinOnColumns(), tableName, fromItemAliasName);
    }

    private void updateQueryRecordWithTableName(Set<ColumnRecord> columnRecords, String tableName, String fromItemAliasName) {
        //直接修改hashset中的元素，会导致iterator.remove失效，所以先删除后添加
        Iterator<ColumnRecord> iterator = columnRecords.iterator();
        Set<ColumnRecord> temp = new HashSet<>();
        while (iterator.hasNext()) {
            ColumnRecord columnRecord = iterator.next();
            if (columnRecord.getTableAlias() != null && columnRecord.getTableAlias().equals(fromItemAliasName)) {
                ColumnRecord columnRecord2 = SerializationUtils.clone(columnRecord);
                columnRecord2.setTableAlias(tableName);
                columnRecord2.setFinishAnalyse(true);
                temp.add(columnRecord2);
                iterator.remove();
            }
        }
        columnRecords.addAll(temp);
    }


    private List<ColumnRecord> analyzeExpression(int curQueryNum, Expression expression, Alias columnAlias) {
        //当前表达式中涉及到的COLUMN
        List<ColumnRecord> columns = new ArrayList<>();
        if (expression == null) {
            return columns;
        }
        if (columnAlias == null) {
            columnAlias = new Alias(expression.toString().contains(".") ? expression.toString().split("\\.")[1] : expression.toString());
        }
        if (isConstantValue(expression)) {
            ColumnRecord columnRecord = new ColumnRecord();
            columnRecord.setColumnName("@constant_value@");
            columnRecord.setColumnAlias(columnAlias.getName());
            columnRecord.setFinishAnalyse(true);
            columns.add(columnRecord);
            return columns;
        }
        if (expression instanceof Column) {
            Column column = (Column) expression;
            ColumnRecord columnRecord = new ColumnRecord();
            columnRecord.setColumnName(column.getColumnName());
            columnRecord.setColumnAlias(columnAlias.getName());
            columnRecord.setTableAlias(column.getTable() == null ? null : column.getTable().getName());
            columns.add(columnRecord);
        } else if (expression instanceof Function) {
            Function function = (Function) expression;
            if (function.getParameters() == null) {
                return columns;
            }
            List<Expression> expressions = function.getParameters().getExpressions();
            for (Expression ex : expressions) {
                columns.addAll(analyzeExpression(curQueryNum, ex, columnAlias));
            }
        } else if (expression instanceof CastExpression) {
            CastExpression castExpression = (CastExpression) expression;
            columns.addAll(analyzeExpression(curQueryNum, castExpression.getLeftExpression(), columnAlias));
        } else if (expression instanceof CaseExpression) {
            List<WhenClause> whenClauses = ((CaseExpression) expression).getWhenClauses();
            for (WhenClause whenClause : whenClauses) {
                columns.addAll(analyzeExpression(curQueryNum, whenClause.getWhenExpression(), columnAlias));
                columns.addAll(analyzeExpression(curQueryNum, whenClause.getThenExpression(), columnAlias));
            }
            columns.addAll(analyzeExpression(curQueryNum, ((CaseExpression) expression).getElseExpression(), columnAlias));
        } else if (expression instanceof BinaryExpression) {
            BinaryExpression binaryExpression = (BinaryExpression) expression;
            columns.addAll(analyzeExpression(curQueryNum, binaryExpression.getLeftExpression(), columnAlias));
            columns.addAll(analyzeExpression(curQueryNum, binaryExpression.getRightExpression(), columnAlias));
        } else if (expression instanceof Parenthesis) {
            columns.addAll(analyzeExpression(curQueryNum, ((Parenthesis) expression).getExpression(), columnAlias));
        } else if (expression instanceof IsNullExpression) {
            columns.addAll(analyzeExpression(curQueryNum, ((IsNullExpression) expression).getLeftExpression(), columnAlias));
        } else if (expression instanceof InExpression) {
            InExpression inExpression = (InExpression) expression;
            if (inExpression.getLeftExpression() != null) {
                columns.addAll(analyzeExpression(curQueryNum, inExpression.getLeftExpression(), columnAlias));
            }
            if (inExpression.getRightExpression() != null) {
                columns.addAll(analyzeExpression(curQueryNum, inExpression.getRightExpression(), columnAlias));
            }
            if (inExpression.getRightItemsList() != null) {
                ItemsList rightItemsList = inExpression.getRightItemsList();
                if (rightItemsList instanceof SubSelect) {
                    QueryRecord queryRecord = analyzeSelectBodyInner(((SubSelect) rightItemsList).getSelectBody());
                    queryRecord.setParentQueryId(curQueryNum);
                    columns.addAll(queryRecord.getSelectColumns());
                }
            }
        } else if (expression instanceof MySQLGroupConcat) {
            MySQLGroupConcat mySQLGroupConcat = (MySQLGroupConcat) expression;
            for (Expression ex : mySQLGroupConcat.getExpressionList().getExpressions()) {
                columns.addAll(analyzeExpression(curQueryNum, ex, columnAlias));
            }
            List<OrderByElement> orderByElements = mySQLGroupConcat.getOrderByElements();
            if (CollectionUtils.isNotEmpty(orderByElements)) {
                for (OrderByElement orderByElement : orderByElements) {
                    columns.addAll(analyzeExpression(curQueryNum, orderByElement.getExpression(), columnAlias));
                }
            }
        } else if (expression instanceof Between) {
            Between between = (Between) expression;
            columns.addAll(analyzeExpression(curQueryNum, between.getLeftExpression(), columnAlias));
            columns.addAll(analyzeExpression(curQueryNum, between.getBetweenExpressionStart(), columnAlias));
            columns.addAll(analyzeExpression(curQueryNum, between.getBetweenExpressionEnd(), columnAlias));
        } else if (expression instanceof NotExpression) {
            columns.addAll(analyzeExpression(curQueryNum, ((NotExpression) expression).getExpression(), columnAlias));
        } else if (expression instanceof ExistsExpression) {
            columns.addAll(analyzeExpression(curQueryNum, ((ExistsExpression) expression).getRightExpression(), columnAlias));
        } else if (expression instanceof SubSelect) {
            //这里的expression不会是SelectItem，SelectItem在PlainSelect中额外处理
            QueryRecord queryRecord = analyzeSelectBodyInner(((SubSelect) expression).getSelectBody());
            queryRecord.setParentQueryId(curQueryNum);
            globalQueryRecords.get(curQueryNum).getChildQueries().add(queryRecord);
        } else if (expression instanceof RowConstructor) {
            RowConstructor rowConstructor = (RowConstructor) expression;
            List<Expression> expressions = rowConstructor.getExprList().getExpressions();
            for (Expression ex : expressions) {
                columns.addAll(analyzeExpression(curQueryNum, ex, columnAlias));
            }
        } else if (expression instanceof VariableAssignment) {
            columns.addAll(analyzeExpression(curQueryNum, ((VariableAssignment) expression).getExpression(), columnAlias));
        } else if (expression instanceof IntervalExpression) {
            columns.addAll(analyzeExpression(curQueryNum, ((IntervalExpression) expression).getExpression(), columnAlias));
        } else if (expression instanceof SignedExpression) {
            columns.addAll(analyzeExpression(curQueryNum, ((SignedExpression) expression).getExpression(), columnAlias));
        } else if (expression instanceof JsonFunction){
            ArrayList<JsonFunctionExpression> expressions = ((JsonFunction) expression).getExpressions();
            for (JsonFunctionExpression jsonFunctionExpression: expressions){
                columns.addAll(analyzeExpression(curQueryNum, jsonFunctionExpression.getExpression(), columnAlias));
            }
        }else {
            log.error("暂不支持的表达式类型：{},示例：{}", expression.getClass(), expression);
            ColumnRecord columnRecord = new ColumnRecord();
            columnRecord.setColumnName(expression.toString());
            columnRecord.setTableAlias(columnAlias.getName());
            columns.add(columnRecord);
        }
        return columns;
    }

    private boolean isConstantValue(Expression expression) {
        return expression instanceof StringValue
                || expression instanceof DoubleValue
                || expression instanceof LongValue
                || expression instanceof NullValue
                || expression instanceof TimeKeyExpression
                || expression instanceof UserVariable;
    }

    private List<String> getColumnNamesFromTableName(String tableName) throws Exception {
        if (StringUtils.isEmpty(tableName)) {
            return new ArrayList<>();
        }
        List<String> columnNames = new ArrayList<>();
        ResultSet resultSet = metaData.getColumns(catalog, null, tableName, null);
        while (resultSet.next()) {
            columnNames.add(resultSet.getString("COLUMN_NAME"));
        }
        resultSet.close();
        return columnNames;
    }

    private String format(String str) {
        if (StringUtils.isNotEmpty(str)) {
            return str.toLowerCase().replace("`", "").replace("'", "");
        }
        return str;
    }

}
