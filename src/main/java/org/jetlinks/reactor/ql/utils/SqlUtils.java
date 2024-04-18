package org.jetlinks.reactor.ql.utils;

public class SqlUtils {

    public static String getCleanStr(String str) {
        if (str == null) {
            return null;
        }
        char first = str.charAt(0);
        char end = str.charAt(str.length() - 1);

        boolean startWith = first == '\"' || first == '`';
        boolean endWith = end == '\"' || end == '`';

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
