package cn.wxn;

import net.sf.jsqlparser.JSQLParserException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * @author wxn
 * @since 2023/8/10
 */
public class SelectTest {
    private SQLLineageHelper sqlLineageHelper;

    private SQLAnalyseHelper sqlAnalyseHelper;
    @BeforeEach
    public void initAnalyzer(){
        sqlLineageHelper = new SQLLineageHelper();
        sqlAnalyseHelper = new SQLAnalyseHelper();
    }
    @Test
    public void simpleSelect() throws JSQLParserException {
        String sql = "select f1, f2 from table1";
        System.out.println(sqlLineageHelper.sqlLineageAnalyse(sql));
    }

    @Test
    public void functionSelect() throws JSQLParserException {
        String sql = "select f1,concat_ws(f2,f3,f4) as rf1, case when min(f3) < 3 then 1 else 2 end as f4, (select f5 from table2) from table1 as a";
        System.out.println(sqlLineageHelper.sqlLineageAnalyse(sql));;
    }

    @Test
    public void joinOnSelect() throws JSQLParserException {
//        String sql = "select a.f1, b.f2 from table1 as a join table2 as b";
        String sql = "select a.f1, b.f2 from table1 a join (select f2 from table2) as b";
//        String sql = "select k.f1, k.f2 from (table1 a join (select f2 from table2) as b) as k";
        System.out.println(sqlLineageHelper.sqlLineageAnalyse(sql));
    }

    @Test
    public void subJoinSelect() throws JSQLParserException {
//        String sql = "select f1,(select f3 from table3) from ((select f1 from table1) join( select f2 from table2))";
        String sql = "select f2 from (table1 join( select f2 from table2))";
        System.out.println(sqlLineageHelper.sqlLineageAnalyse(sql));
    }

    @Test
    public void selectStar() throws JSQLParserException{
        String sql = "select * from (select * from (select * from ajxx)a)aa";
        System.out.println(sqlAnalyseHelper.sqlAnalyse(sql));
    }

    @Test
    public void union() throws JSQLParserException{
        String sql = "select * from (select * from (select a1 from t1 union all select a1 from t2))";
        System.out.println(sqlLineageHelper.sqlLineageAnalyse(sql));
    }

    @Test
    public void testStar() throws JSQLParserException {
        String sql = "select * from (select * from (select * from gfjyhtxx) t1 join (select * from bzxzfxx) t2)";
        sqlAnalyseHelper.sqlAnalyse(sql);
    }

}
