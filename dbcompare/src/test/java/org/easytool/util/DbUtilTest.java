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
            
            put("fssbcs", new DB("fssbcs", "db2", "fssb", "Fscs@0901"
                    , new DBUrl("db2", "fssbcs", "200.30.10.101", "50000")));

            put("fssbcs204", new DB("fssbjcs", "db2", "fssb", "fssb#2018"
                    , new DBUrl("db2", "fssbjcs", "189.30.100.204", "50000")));
            
            put("fssbjdb63", new DB("fssbjdb", "db2", "dev_liqj", "Lqj#20190201"
                    , new DBUrl("db2", "fssbjdb", "189.30.100.63", "50000")));

            put("icdb_tjbb", new DB("icdb_tjbb", "oracle", "tjbb", "fssitjbb"
                    , new DBUrl("oracle-tns", "icdb", "189.30.100.54", "1521")));

            put("db2_test_2_local_a53s", new DB("test_2", "db2", "liqj", "unisure"
                    , new DBUrl("db2", "test_2", "127.0.0.1", "50000")));
        }
    };

}
