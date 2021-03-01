package io.hubbox;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.io.FileUtils;

/**
 *
 * @author Fatih
 */
public class Constants {

    static String jar_path;
    public final static String hubbox_dns = "8.8.8.8,8.8.4.4";
    private static String hostname;
//    public static String model;
//    public static String version;

    public static final String vpncmd_path = "/root/vpnbridge/vpncmd";
    public static String vpncmd_password = "vpnbridge1.";
    //    static ModbusClient modbusClient;
    static String custom_title;
    public static String hubbox_model;



    public static String get_jar_path() {
        if (jar_path != null) {
            return jar_path;
        } else {
            try {
                //            File jarPath = new File(Constants.class.getProtectionDomain().getCodeSource().getLocation().getPath());
//            jar_path = jarPath.getParentFile().getAbsolutePath();
                File jarPath = new File(Constants.class.getProtectionDomain().getCodeSource().getLocation().toURI());
                jar_path = jarPath.getParentFile().getAbsolutePath();
            } catch (URISyntaxException ex) {
                Logger.getLogger(Constants.class.getName()).log(Level.SEVERE, null, ex);
            }

            return jar_path;
        }
    }

    public static void init_hostname() {
        try {
            hostname = FileUtils.readFileToString(new File("/etc/hostname"), "UTF8");
        } catch (IOException ex) {
            Logger.getLogger(Constants.class.getName()).log(Level.SEVERE, null, ex);
        }

//        switch (model) {
//            case "R1":
////                modbusClient = OrangeMain.modbusClient;
//                break;
//            case "X1":
////                modbusClient = OrangeMain_X1.modbusClient;
//                break;
//        }
    }

    public static String getHostname() {
        if (hostname == null) {
            init_hostname();

        }
        return hostname;

    }

//    static void update() {
//        switch (model) {
//            case "R1":
//                OrangeMain.update();
//                break;
//            case "X1":
//                OrangeMain_X1.update();
//                break;
//
//        }
//    }

    public static void set_custom_title(String string) {
        custom_title = string;
//        Virtual_Usb_Helper.setProperty("ServerName", custom_title);
//        Virtual_Usb_Helper.restart();
    }

}
