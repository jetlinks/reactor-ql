package org.jetlinks.reactor.ql.utils;

public class SqlUtils {

    public static String getCleanStr(String str) {
        if (str.startsWith("\"")) {
            str = str.substring(1);
        }
        if (str.endsWith("\"")) {
            str = str.substring(0, str.length() - 1);
        }
        return str;
    }

}
