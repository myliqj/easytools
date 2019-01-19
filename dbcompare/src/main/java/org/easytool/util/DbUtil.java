package org.easytool.util;

public class DbUtil {

    public String getValue(Object obj,String nullVal){
        if (obj==null) return nullVal;

        if (obj instanceof String){
            return obj.toString();
        } else if (obj instanceof java.sql.Timestamp) {
            return DateUtils.formatDateTime((java.sql.Timestamp) obj);
        } else if (obj instanceof java.sql.Date) {
            return DateUtils.formatDateTime((java.sql.Date) obj);
        } else if (obj instanceof java.sql.Time) {
            return DateUtils.formatDateTime((java.sql.Time) obj);
        } else if (obj instanceof oracle.sql.TIMESTAMP) {
            try { return DateUtils.formatDateTime(((oracle.sql.TIMESTAMP) obj).timestampValue());
            } catch (Exception e) {}
        }
        return obj.toString();
    }
}
