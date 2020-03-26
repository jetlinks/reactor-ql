package org.jetlinks.reactor.ql;

import lombok.SneakyThrows;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;

public class SqlTest {

    @SneakyThrows
    public static void main(String[] args) {

        Statement statement = CCJSqlParserUtil.parse(String.join(" ", "",
            "select t1.deviceId,t1.properties.temp,",
            "case t1.properties.type when 1 then 'online' when 2 then 'offline' else 'unknown' end type",
            "from \"/device/*/message/property/*\" t1,",
            "\"/device/*/message/event\" t2 ",
            "where t1.deviceId = t2.deviceId and val > now() and a >0" ,
            "group by interval('1s')"
        ));



        SelectBody body = ((Select) statement).getSelectBody();

        PlainSelect select = ((PlainSelect) body);


        System.out.println(select.toString());

    }
}
