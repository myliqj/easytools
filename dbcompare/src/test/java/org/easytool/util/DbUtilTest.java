package org.easytool.util;

import org.easytool.dbcompare.model.DB;
import org.easytool.dbcompare.model.DBUrl;

import java.util.HashMap;
import java.util.Map;

public class DbUtilTest {

    public static Map<String,DB> testDB = new HashMap<String, DB>(){
        {
            put("db2_test2_local", new DB("test2", "db2", "db2admin", "db2admin"
                    , new DBUrl("db2", "test2", "127.0.0.1", "50000")));
        }
    };

}
