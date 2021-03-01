package io.hubbox.commons;


import io.hubbox.Constants;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;
import org.apache.commons.dbutils.handlers.ArrayHandler;
import org.apache.commons.dbutils.handlers.ArrayListHandler;
import org.apache.commons.dbutils.handlers.MapListHandler;
import org.apache.commons.io.FileUtils;
import org.h2.jdbcx.JdbcConnectionPool;
import org.h2.jdbcx.JdbcDataSource;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Database_Helper {

    //    static DataSource ds = getDataSource("org.h2.Driver", "jdbc:h2:" + FirstSetup.main_path + "/db/hubbox", "hubbox_bpi", "hub.box.2017");
//    static QueryRunner qr = new QueryRunner(ds);
    static QueryRunner qr;
    private final static Logger logger = Logger.getLogger(Database_Helper.class.getName());
    static boolean backupthreadrunning = false;

    public static void init(String url) {
        JdbcDataSource ds2 = new JdbcDataSource();
//        ds2.setURL("jdbc:h2:./testdb123;SCHEMA=HUBBOX");
//        ds2.setURL("jdbc:h2:" + Init.main_path + "/db/hubbox");
//********
//ds2.setURL("jdbc:h2:" + Constants.get_jar_path() + File.separator + "db" + File.separator + "hubbox");
//***********

//        ds2.setURL("jdbc:h2:" + Constants.get_jar_path() + File.separator + "db" + File.separator + "hubbox" + ";FILE_LOCK=SOCKET");
//        ds2.setURL("jdbc:h2:" + Constants.get_jar_path() + File.separator + "db" + File.separator + "hubbox" + ";FILE_LOCK=SOCKET;TRACE_LEVEL_FILE=2;TRACE_LEVEL_SYSTEM_OUT=0");
//        ds2.setURL("jdbc:h2:" + "C:\\Users\\fatih2\\Desktop\\log" + "\\db\\hubbox");
        ds2.setUser("hubbox_opi");
        ds2.setPassword("hub.box.2018");

        ds2.setUrl(url);

        logger.info("init database");

        if (new File(Constants.get_jar_path() + File.separator + "db" + File.separator + "hubbox.mv.db").exists() == false) {

            logger.info("database dosyasi yok");

            if (new File(Constants.get_jar_path() + File.separator + "db" + File.separator + "backup.zip").exists() == true) {

                logger.info("database yok ama yedek var, restoring");
                org.h2.tools.Restore.execute(Constants.get_jar_path() + File.separator + "db" + File.separator + "backup.zip", Constants.get_jar_path() + File.separator + "db", null);
            }
        } else {
            logger.info("database dosyasi var");
        }

        JdbcConnectionPool cp = JdbcConnectionPool.create(ds2);
        qr = new QueryRunner(cp);

//        qr = new QueryRunner(ds2);
        logger.info("db path " + ds2.getUrl());
        if (backupthreadrunning == false) {
            backupthread();
            backupthreadrunning = true;
        }
    }

    public static void init() {
        init("jdbc:h2:" + Constants.get_jar_path() + File.separator + "db" + File.separator + "hubbox;TRACE_LEVEL_FILE=0");
    }

    static void backupthread() {

        //backup thread 5 dakikada 1
        Thread db_backup_thread = new Thread(new Runnable() {
            @Override
            public void run() {
                for (; ; ) {
                    try {
                        Thread.sleep(5 * 60 * 1000);
                    } catch (InterruptedException ex) {
                        Logger.getLogger(Database_Helper.class.getName()).log(Level.SEVERE, null, ex);
                    }
                    runbackupcmds();
                }
            }

        });
        db_backup_thread.setName("db_backup_thread");
        db_backup_thread.setDaemon(true);
        db_backup_thread.start();
    }

    public static void runbackupcmds() {
        try {
            qr.update("CHECKPOINT SYNC");
        } catch (SQLException ex) {
            Logger.getLogger(Database_Helper.class.getName()).log(Level.SEVERE, null, ex);
        }

        try {
            qr.update("BACKUP TO '" + Constants.get_jar_path() + File.separator + "db" + File.separator + "backup.zip'");
            logger.info("db backup complete");
        } catch (SQLException ex) {
//                        Logger.getLogger(Database_Helper.class.getName()).log(Level.SEVERE, null, ex);
            logger.info("db backup failed " + ex.getMessage());
        }
    }

    public static QueryRunner getQR() {
        return qr;
    }

    public static List<Map<String, Object>> select(String sql) {
//    public static String select(String sql) {

        try {
//            List<Object[]> query = qr.query(sql, new ArrayListHandler());
            List<Map<String, Object>> result = qr.query(sql, new MapListHandler());
//            return new Gson().toJson(result);
            return result;

        } catch (SQLException ex) {
            Logger.getLogger(Database_Helper.class.getName()).log(Level.SEVERE, null, ex);
        }

        return null;
    }

    public static int update(String sql) {

        try {
            return qr.update(sql);

        } catch (SQLException ex) {
            Logger.getLogger(Database_Helper.class.getName()).log(Level.SEVERE, null, ex);
        }

        return -1;

    }

    static void runCreateScripts() throws SQLException {
        qr.update("create schema if not exists HUBBOX");
//            qr.update("CREATE TABLE HUBBOX.CONFIG ("
//                    + "ID INTEGER NOT NULL IDENTITY, "
//                    + "KEY VARCHAR(2147483647) NOT NULL, "
//                    + "VALUE VARCHAR(2147483647) NOT NULL, "
//                    + "PRIMARY KEY (ID), "
//                    + "CONSTRAINT CONFIG_IX1 UNIQUE (KEY));");

        qr.update("CREATE TABLE if not exists HUBBOX.CUSTOMPROPERTY "
                + "(K VARCHAR(2147483647) PRIMARY KEY, "
                + "V VARCHAR(2147483647), "
                + "T datetime NULL DEFAULT current_timestamp)");

        qr.update("CREATE TABLE if not exists HUBBOX.RS232LOG(C_ID INT PRIMARY KEY auto_increment,"
                + "C_DATE datetime DEFAULT current_timestamp,"
                + "C_LINE VARCHAR(2147483647))");

        qr.update("CREATE TABLE if not exists HUBBOX.SERIALFILTER(C_ID INT PRIMARY KEY auto_increment,"
                + "C_DATE datetime DEFAULT current_timestamp,"
                + "C_QUERY VARCHAR(2147483647),C_ACTION VARCHAR(2147483647));");

        qr.update("CREATE TABLE if not exists HUBBOX.TASK(C_ID INT PRIMARY KEY auto_increment, "
                + "C_NAME VARCHAR(255),C_START VARCHAR(2147483647),C_REPEAT VARCHAR(2147483647),"
                + "C_PRECONDITIONS VARCHAR(2147483647),C_POSTCONDITIONS VARCHAR(2147483647),"
                + "C_DATASOURCE VARCHAR(2147483647),C_ACTION VARCHAR(2147483647),C_NOTIFY VARCHAR(2147483647),"
                + "C_ENABLED BOOLEAN DEFAULT false,C_ARCHIVE BOOLEAN DEFAULT false)");

        qr.update("CREATE TABLE if not exists HUBBOX.OFFLINE_DATA(C_ID INT PRIMARY KEY auto_increment,"
                + "C_DATE datetime DEFAULT current_timestamp,C_DATA VARCHAR(2147483647),C_TYPE VARCHAR(255))");
    }

    public static boolean createDB() {

        boolean result = false;
        try {
            runCreateScripts();

            result = true;
//        } catch (SQLException ex) {
//            Logger.getLogger(Database_Helper.class.getName()).log(Level.SEVERE, null, ex);
//        } catch (IllegalStateException ex2) {
        } catch (Exception ex) {
            logger.info("db file hatali trying to recover");
            try {
                qr.update("SHUTDOWN");
            } catch (SQLException ex2) {

            }
            File file = new File(Constants.get_jar_path() + File.separator + "db" + File.separator + "hubbox.mv.db");
//            File file2 = new File(Constants.get_jar_path() + File.separator + "db" + File.separator + "hubbox.trace.db");
            file.delete();

            logger.info("deleted db file");
//            file2.delete();
//            init("jdbc:h2:" + Constants.get_jar_path() + File.separator + "db" + File.separator + "hubbox");

//            java org.h2.tools.RunScript -url jdbc:h2:~/test -user sa -script test.zip -options compression zip
            if (new File(Constants.get_jar_path() + File.separator + "db" + File.separator + "backup.zip").exists()) {
                long currentTimeMillis = System.currentTimeMillis();
                logger.info("restoring backup db " + currentTimeMillis);
                try {
                    logger.info("snapshot of backup created");
                    FileUtils.copyFile(new File(Constants.get_jar_path() + File.separator + "db" + File.separator + "backup.zip"),
                            new File(Constants.get_jar_path() + File.separator + "db" + File.separator + currentTimeMillis + "_backup.zip"));
                } catch (IOException ex1) {
                    Logger.getLogger(Database_Helper.class.getName()).log(Level.SEVERE, null, ex1);
                }
                org.h2.tools.Restore.execute(Constants.get_jar_path() + File.separator + "db" + File.separator + "backup.zip", Constants.get_jar_path() + File.separator + "db", null);
            }
//            init("jdbc:h2:" + Constants.get_jar_path() + File.separator + "db" + File.separator + "hubbox");
            init();
            try {

                runCreateScripts();
                logger.info("restoredan sonraki task sayisi = " + (qr.query("select count(*) FROM HUBBOX.TASK", new ArrayHandler()))[0]);
                result = true;
            } catch (SQLException ex3) {
                Logger.getLogger(Database_Helper.class.getName()).log(Level.SEVERE, null, ex3);

            }

        }
        return result;

    }

    public static void setCustomProperty(String key, String value) {
        try {
            int update = qr.update("update HUBBOX.CUSTOMPROPERTY set V = ?,t=CURRENT_TIMESTAMP() where K = ? ", value, key);
            if (update == 0) {
                qr.update("insert into HUBBOX.CUSTOMPROPERTY(K,V) VALUES(?,?)", new Object[]{key, value});
            }
        } catch (SQLException ex) {
            Logger.getLogger(Database_Helper.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public static void setCustomProperty(String key, Object value) {
        try {
            int update = qr.update("update HUBBOX.CUSTOMPROPERTY set V = ?,t=CURRENT_TIMESTAMP() where K = ? ", new Object[]{String.valueOf(value), key});

            if (update == 0) {
                qr.update("insert into HUBBOX.CUSTOMPROPERTY(K,V) VALUES(?,?)", new Object[]{key, String.valueOf(value)});
            }
        } catch (SQLException ex) {
            Logger.getLogger(Database_Helper.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public static Object[] getCustomProperty(String key) {
        try {
            Object[] sonuc = qr.query("select V,T FROM HUBBOX.CUSTOMPROPERTY where K = ?", new ArrayHandler(), key);
            if (sonuc.length > 0) {
                return (sonuc);
            }
        } catch (SQLException ex) {
//            Logger.getLogger(DatabaseHelper.class.getName()).log(Level.SEVERE, null, ex);
        }
        return null;
    }

    public static <Z> Z getCustomProperty(String key, Class<Z> var) {

        String s = (var.getCanonicalName());

        try {
            Object[] sonuc = (Object[]) qr.query("select V FROM HUBBOX.CUSTOMPROPERTY where K = ?", (ResultSetHandler) new ArrayHandler(), new Object[]{key});
            if (sonuc.length > 0) {
                switch (s) {
                    case "java.lang.Boolean":
                        return (Z) new Boolean((String) sonuc[0]);
                    case "java.lang.Integer":
                        return (Z) new Integer((String) sonuc[0]);
                    default:
                        return (Z) ((String) sonuc[0]);
                }
//                return (Z) (sonuc[0]);

            }
        } catch (SQLException ex) {
//            Logger.getLogger(DatabaseHelper.class.getName()).log(Level.SEVERE, null, ex);
        }
        return null;
    }

    public static List<Object[]> getCustomProperties(String keylist) {
        String[] split = keylist.split(",");
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < split.length; i++) {
            sb.append("?,");
        }
        sb.deleteCharAt(sb.length() - 1);

        try {
            List<Object[]> rows = (List<Object[]>) qr.query("select K,V,T FROM HUBBOX.CUSTOMPROPERTY where K IN (" + sb.toString() + ")", (ResultSetHandler) new ArrayListHandler(), (Object[]) split);
            return rows;
        } catch (SQLException ex) {
//            Logger.getLogger(DatabaseHelper.class.getName()).log(Level.SEVERE, null, ex);
        }
        return null;

//        try {
//            List<Map<String, Object>> result = qr.query("SELECT * FROM HUBBOX.RS232LOG where C_LINE IN ("+sb.toString()+")", new MapListHandler(),split);
////            List<Map<String, Object>> result = qr.query("SELECT * FROM HUBBOX.RS232LOG where C_LINE IN ('hasan','osman')", new MapListHandler());
////            List<Map<String, Object>> result = qr.query("SELECT * FROM HUBBOX.RS232LOG ", new MapListHandler());
//            System.out.println("selecttest size: "+result.size());
//            for (Map<String, Object> map : result) {
//                for (Entry<String, Object> entry : map.entrySet()) {
//                    System.out.println(entry.getKey()+":"+entry.getValue());
//                }
//
//                System.out.println("***");
//            }
////            return result;
//
//        } catch (SQLException ex) {
//            Logger.getLogger(Database_Helper.class.getName()).log(Level.SEVERE, null, ex);
//        }
//
////        return null;
//    }
    }

    //    public static Map<String, String> listCustomProperties() {
//        HashMap<String, String> hm = new HashMap<String, String>();
//        try {
//            List<Object[]> rows = qr.query("select * FROM HUBBOX.CUSTOMPROPERTY", new ArrayListHandler());
//            for (Object[] row : rows) {
////                System.out.println(row[0] + " : " + row[1]);
//                hm.put((String) row[0], (String) row[1]);
//            }
//        } catch (SQLException ex) {
//            Logger.getLogger(Database_Helper.class.getName()).log(Level.SEVERE, null, ex);
//        }
//        return hm;
//    }
//    static DataSource ds2 = getDataSource("org.h2.Driver", "jdbc:h2:./db/hubbox_test", "a", "b");
//    static QueryRunner qr2 = new QueryRunner(ds2);
    //gelen her seri port mesajini loglar
    public static boolean addRS232Log(String line) {
        boolean hata1 = false;
//        System.out.println("--adding RS232 log " + line);

        try {
            qr.update("INSERT INTO HUBBOX.RS232LOG(C_LINE) values(?)", line);
            qr.update("DELETE FROM HUBBOX.RS232LOG "
                    + "WHERE C_ID < (SELECT (MAX(C_ID) - 1000))");

            hata1 = false;
        } catch (SQLException ex) {
            hata1 = true;
            Logger.getLogger(Database_Helper.class.getName()).log(Level.SEVERE, null, ex);
        }
        return (hata1);
    }

    public static Map<String, String> listCustomProperties() {
        HashMap<String, String> hm = new HashMap<>();
        try {
            List<Object[]> rows = (List<Object[]>) qr.query("select * FROM HUBBOX.CUSTOMPROPERTY", (ResultSetHandler) new ArrayListHandler());
            for (Object[] row : rows) {
                hm.put((String) row[0], (String) row[1]);
            }
        } catch (SQLException ex) {
            Logger.getLogger(Database_Helper.class.getName()).log(Level.SEVERE, null, ex);
        }
        return hm;
    }

    public static void clearRS232Logs() {
        try {
            qr.update("TRUNCATE TABLE HUBBOX.RS232LOG RESTART IDENTITY");
        } catch (SQLException ex) {
            Logger.getLogger(Database_Helper.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    // com port dan gelen mesaj hangi alarmlari tetikleyecek bulur
    public static List<String> findSerialFilters(String comMessage) {
        ArrayList<String> sonuclar = new ArrayList<String>();
        try {
            List<Object[]> query = (List<Object[]>) qr.query("SELECT C_ACTION FROM HUBBOX.SERIALFILTER where ? LIKE CONCAT('%',C_QUERY,'%')", (ResultSetHandler) new ArrayListHandler(), new Object[]{comMessage});

            for (Object[] objects : query) {
                sonuclar.add((String) objects[0]);
            }

        } catch (SQLException ex) {
            Logger.getLogger(Database_Helper.class.getName()).log(Level.SEVERE, null, ex);
        }

        return sonuclar;

    }

    // butun filtreler
    public static List<Object[]> listSerialFilters() {
//        C_ID INT PRIMARY KEY auto_increment,"
//                    + "C_DATE datetime DEFAULT current_timestamp,"
//                    + "C_QUERY VARCHAR(2147483647),"
//                    + "C_ACTION
        ArrayList<Object[]> sonuclar = new ArrayList<Object[]>();
        try {
            List<Object[]> query = (List<Object[]>) qr.query("SELECT C_ID,C_DATE,C_QUERY,C_ACTION FROM HUBBOX.SERIALFILTER", (ResultSetHandler) new ArrayListHandler());

            sonuclar.addAll(query);

        } catch (SQLException ex) {
            Logger.getLogger(Database_Helper.class.getName()).log(Level.SEVERE, null, ex);
        }

        return sonuclar;

    }

    public static void deleteRS232Filter(int id) {
        try {
//            System.out.println(qr.query("select count(*) from HUBBOX.SERIALFILTER", new ArrayHandler())[0]);
//            System.out.println("deleting " + id);
            qr.update("DELETE FROM HUBBOX.SERIALFILTER where C_ID = ?", id);
//            System.out.println(qr.query("select count(*) from HUBBOX.SERIALFILTER", new ArrayHandler())[0]);
        } catch (SQLException ex) {
            Logger.getLogger(Database_Helper.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public static void updateRS232Filter(int id, String query, String action) {
        try {
            qr.update("UPDATE HUBBOX.SERIALFILTER SET C_QUERY = ?, C_ACTION = ?, C_DATE = CURRENT_TIMESTAMP() WHERE C_ID = ? ", query, action, id);
        } catch (SQLException ex) {
            Logger.getLogger(Database_Helper.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public static void addRS232Filter(String query, String action) {
        try {
            qr.update("INSERT INTO HUBBOX.SERIALFILTER(C_QUERY,C_ACTION) VALUES(?,?)", query, action);
        } catch (SQLException ex) {
            Logger.getLogger(Database_Helper.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    // com port dan gelen mesajlar
    public static List<Object[]> getRS232Messages(int limit) {
        ArrayList<Object[]> sonuclar = new ArrayList<Object[]>();

//          qr.update("CREATE TABLE if not exists HUBBOX.RS232LOG("
//                    + "C_ID INT PRIMARY KEY auto_increment,"
//                    + "C_DATE datetime DEFAULT current_timestamp,"
//                    + "C_LINE VARCHAR(2147483647))");
        try {
            List<Object[]> query = qr.query("SELECT C_DATE,C_LINE FROM HUBBOX.RS232LOG ORDER BY C_ID DESC LIMIT ?", new ArrayListHandler(), limit);
//            System.out.println("--getrs232message b "+query.size());

//            for (Object[] objects : query) {
//                System.out.println("--printing rs232 log "+objects[0]+" "+objects[1]);
//                sonuclar.add(new String[]{(String) (objects[0]), (String) (objects[1])});
//                sonuclar.add(objects);
//            }
            sonuclar.addAll(query);

        } catch (SQLException ex) {
            Logger.getLogger(Database_Helper.class.getName()).log(Level.SEVERE, null, ex);
        }

        return sonuclar;

    }

    public static void printTable(String table_name) {
        QueryRunner qr = getQR();
        try {
            List<Object[]> result = (List<Object[]>) qr.query("select * from HUBBOX." + table_name, (ResultSetHandler) new ArrayListHandler());
            for (Object[] objects : result) {
                for (Object object : objects) {
                    System.out.print(object + "\t");
                }
                System.out.println("");
            }
        } catch (SQLException ex) {
            Logger.getLogger(Database_Helper.class.getName()).log(Level.SEVERE, null, ex);
        }

    }

    public static void main(String[] args) {

        init();
//        createDB();
//        try {
//            qr.update("insert into hubbox.customproperty(K,V) values(?,?)", "ali3", "veli3");
//        } catch (SQLException ex) {
//            Logger.getLogger(Database_Helper.class.getName()).log(Level.SEVERE, null, ex);
//        }
        printTable("CUSTOMPROPERTY");
        System.out.println("****************");
        try {
            qr.update("SHUTDOWN");
        } catch (SQLException ex) {
            Logger.getLogger(Database_Helper.class.getName()).log(Level.SEVERE, null, ex);
        }
//        try {
//            qr.update("BACKUP TO 'backup.zip'");
//        } catch (SQLException ex) {
//            Logger.getLogger(Database_Helper.class.getName()).log(Level.SEVERE, null, ex);
//        }
        org.h2.tools.Restore.execute("backup.zip", Constants.get_jar_path() + File.separator + "db", null);
        init();
        printTable("CUSTOMPROPERTY");

    }

}

