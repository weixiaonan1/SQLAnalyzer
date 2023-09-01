package cn.wxn;

import lombok.extern.slf4j.Slf4j;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.Select;
import org.apache.commons.collections4.CollectionUtils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.*;
import java.util.stream.Collectors;

/**
 * 用于血缘解析, 只分析SELECT语句最外层的查询column的来源
 * @author wxn
 * @since 2023/8/10
 */
@Slf4j
public class SQLLineageHelper {
    public Map<String, Set<String>> sqlLineageAnalyse(String sqlText) throws JSQLParserException {
        Statement statement = CCJSqlParserUtil.parse(sqlText);
        SQLSelectAnalyzer sqlSelectAnalyzer = new SQLSelectAnalyzer();
        Select selectStatement = (Select) statement;
        Map<Integer, QueryRecord> queryRecords;
        try(Connection connection = DriverManager.getConnection("jdbc:mysql://10.60.18.7:29306/data_reporting_baseline?useUnicode=true&characterEncoding=UTF-8&allowMultiQueries=true&useSSL=false&serverTimezone=GMT%2B8",
                "standard_tool","o1aUmQHVkiPyBo")) {
            queryRecords = sqlSelectAnalyzer.analyzeSelectBody(selectStatement.getSelectBody(), connection);
        }catch (Exception e){
            log.error("获取数据库连接失败:", e);
            queryRecords = sqlSelectAnalyzer.analyzeSelectBody(selectStatement.getSelectBody());
        }
        QueryRecord queryRecord1 = queryRecords.get(1);
        Set<ColumnRecord> selectColumns = queryRecord1.getSelectColumns();
        //获取最外层的查询column的表字段来源，用于表之间的字段血缘分析
        Map<String, Set<String>> selectColumnsMap = new HashMap<>();
        Map<String, List<ColumnRecord>> collect = selectColumns.stream().collect(Collectors.groupingBy(ColumnRecord::getColumnAlias));
        for (String outputColumnName: collect.keySet()){
            Set<String> result = new HashSet<>();
            for (ColumnRecord columnRecord: collect.get(outputColumnName)){
                result.addAll(columnSourceAnalyze(columnRecord));
            }
            selectColumnsMap.put(outputColumnName, result);
        }
        return selectColumnsMap;
    }

    private Set<String> columnSourceAnalyze(ColumnRecord columnRecord){
        Set<String> result = new HashSet<>();
        if (CollectionUtils.isEmpty(columnRecord.getSourceColumns())){
            result.add(String.format("%s.%s", columnRecord.getTableAlias(), columnRecord.getColumnName()));
            return result;
        }
        for (ColumnRecord c: columnRecord.getSourceColumns()){
            result.addAll(columnSourceAnalyze(c));
        }
        return result;
    }
}
