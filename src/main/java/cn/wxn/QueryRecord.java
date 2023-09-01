package cn.wxn;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author wxn
 * @since 2023/8/10
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class QueryRecord implements Serializable {
    private int queryId;
    private int parentQueryId = 0;
    private String queryAlias;
    //单独拎出来处理，因为SELECT语句中的子查询和其他地方的子查询不一样，不对parent Query有影响
    private boolean subQueryInSelect = false;
    //单独拎出来处理，因为FROM语句中的子查询需要对parent query的select * 做处理
    private boolean subQueryInFrom = false;
    private List<QueryRecord> childQueries = new ArrayList<>();
    private Set<ColumnRecord> selectColumns = new HashSet<>();
    private Set<ColumnRecord> whereColumns = new HashSet<>();
    private Set<ColumnRecord> groupByColumns = new HashSet<>();
    private Set<ColumnRecord> joinOnColumns = new HashSet<>();
    private Set<ColumnRecord> orderByColumns = new HashSet<>();
    private Set<ColumnRecord> havingColumns = new HashSet<>();

    public void setQueryAlias(String queryAlias) {
        this.queryAlias = format(queryAlias);
    }

    private String format(String str){
        return str.toLowerCase().replace("`","").replace("'","");
    }
}
