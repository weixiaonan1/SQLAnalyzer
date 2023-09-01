package cn.wxn;

import lombok.*;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * @author wxn
 * @since 2023/8/9
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class ColumnRecord implements Serializable {
    /**
     * column name
     */
    private String columnName;
    /**
     * column alias
     */
    private String columnAlias;
    /**
     * table name or table alias
     */
    private String tableAlias;
    /**
     * 是否已经处理结束
     */
    private boolean finishAnalyse = false;
    /**
     * 记录字段来源于子查询的哪些字段
     */
    private List<ColumnRecord> sourceColumns = new ArrayList<>();

    public void setColumnName(String columnName) {
        this.columnName = format(columnName);
    }

    public void setColumnAlias(String columnAlias) {
        this.columnAlias = format(columnAlias);
    }

    public void setTableAlias(String tableAlias) {
        this.tableAlias = format(tableAlias);
    }

    private String format(String str){
        if (StringUtils.isNotEmpty(str)){
            return str.toLowerCase().replace("`","").replace("'","");
        }
        return str;
    }
    @Override
    public String toString() {
        return "ColumnRecord{" +
                "columnName='" + columnName + '\'' +
                ", columnAlias='" + columnAlias + '\'' +
                ", tableAlias='" + tableAlias + '\'' +
                '}';
    }
}
