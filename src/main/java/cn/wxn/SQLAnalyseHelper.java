package cn.wxn;

import lombok.extern.slf4j.Slf4j;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.drop.Drop;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.Select;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * 用于SQL解析，分析查询语句中所有Column的来源
 *
 * @author wxn
 * @since 2023/8/10
 */
@Slf4j
public class SQLAnalyseHelper {
    private final Set<String> tableAndColumns = new HashSet<>();

    String pattern = "--.*$";

    Pattern regex = Pattern.compile(pattern, Pattern.MULTILINE);

    public Set<String> sqlAnalyse(String sqlText) throws JSQLParserException {
        tableAndColumns.clear();
        //预处理SQL
        sqlText = cleanSQLText(sqlText);
        String[] sqls;
        if (sqlText.contains(";")) {
            sqls = StringUtils.split(sqlText, ";");
        } else {
            sqls = new String[]{sqlText};
        }

        Select selectStatement;
        Statement stmt;
        for (String sql : sqls) {
            //只分析SELECT语句
            if (StringUtils.isEmpty(sql) || "\n".equals(sql) || !StringUtils.containsIgnoreCase(sql, "SELECT")) {
                continue;
            }
            try {
                stmt = CCJSqlParserUtil.parse(sql);
            } catch (Exception e) {
                log.error("CCJSqlParserUtil解析SQL文本时出错：\n{}", sql, e);
                continue;
            }
            if (stmt instanceof Drop) {
                continue;
            }
            if (stmt instanceof CreateTable) {
                selectStatement = ((CreateTable) stmt).getSelect();
            } else if (stmt instanceof Insert) {
                selectStatement = ((Insert) stmt).getSelect();
            } else if (stmt instanceof Select) {
                selectStatement = (Select) stmt;
            } else {
                log.error("未能识别的Statement：{}", stmt.getClass());
                continue;
            }
            SQLSelectAnalyzer sqlSelectAnalyzer = new SQLSelectAnalyzer();
            Map<Integer, QueryRecord> queryRecords;
            try (Connection connection = DriverManager.getConnection("jdbc:mysql://localhost:3306",
                    "root", "root")) {
                queryRecords = sqlSelectAnalyzer.analyzeSelectBody(selectStatement.getSelectBody(), connection);
            } catch (SQLException e) {
                log.error("获取数据库连接失败, 仅使用文本解析");
                queryRecords = sqlSelectAnalyzer.analyzeSelectBody(selectStatement.getSelectBody());
            } catch (Exception e) {
                log.error("SQL解析失败：{}", sql, e);
                continue;
            }
            for (QueryRecord queryRecord : queryRecords.values()) {
                for (ColumnRecord cr : queryRecord.getSelectColumns()) {
                    columnSourceAnalyze(cr);
                }
                for (ColumnRecord cr : queryRecord.getWhereColumns()) {
                    columnSourceAnalyze(cr);
                }
                for (ColumnRecord cr : queryRecord.getOrderByColumns()) {
                    columnSourceAnalyze(cr);
                }
                for (ColumnRecord cr : queryRecord.getJoinOnColumns()) {
                    columnSourceAnalyze(cr);
                }
                for (ColumnRecord cr : queryRecord.getHavingColumns()) {
                    columnSourceAnalyze(cr);
                }
                for (ColumnRecord cr : queryRecord.getGroupByColumns()) {
                    columnSourceAnalyze(cr);
                }
            }
        }
        return tableAndColumns;
    }

    private void columnSourceAnalyze(ColumnRecord columnRecord) {
        if (CollectionUtils.isEmpty(columnRecord.getSourceColumns()) && !columnRecord.getColumnName().equals("@constant_value@")) {
            tableAndColumns.add(columnRecord.getTableAlias() + "." + columnRecord.getColumnName());
            return;
        }
        for (ColumnRecord sourceColumn : columnRecord.getSourceColumns()) {
            columnSourceAnalyze(sourceColumn);
        }
    }

    private String cleanSQLText(String sqltxt) {
        sqltxt = sqltxt.replace("\t", "");
        sqltxt = StringUtils.replace(sqltxt, "（", "_");
        sqltxt = StringUtils.replace(sqltxt, "）", "_");
        sqltxt = StringUtils.replace(sqltxt, "、", "或");
        sqltxt = StringUtils.replace(sqltxt, "\"", "'");
        sqltxt = StringUtils.replace(sqltxt, "#", "--");
        sqltxt = removeComment(sqltxt);
        return sqltxt;
    }

    private String removeComment(String sqltxt) {
        String result = regex.matcher(sqltxt).replaceAll("");
        return result;
    }
}
