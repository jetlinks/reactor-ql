package org.jetlinks.reactor.ql.utils;

public class SqlUtils {

    public static String getCleanStr(String str) {
        if (str == null) {
            return null;
        }
        boolean startWith = str.charAt(0) == '\"';
        boolean endWith = str.charAt(str.length() - 1) == '\"';

        if (!startWith && !endWith) {
            return str;
        }

        if (startWith && endWith) {
            return str.substring(1, str.length() - 1);
        }
        if (startWith) {
            return str.substring(1);
        }

        return str.substring(0, str.length() - 1);
    }

}
