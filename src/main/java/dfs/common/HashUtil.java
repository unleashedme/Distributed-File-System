package dfs.common;

import java.io.File;
import java.io.FileInputStream;
import java.security.MessageDigest;


public class HashUtil {

    public static String calculateMD5(File file) throws Exception {

        MessageDigest md = MessageDigest.getInstance("MD5");
        try(FileInputStream fis = new FileInputStream(file)){
            byte[] buffer = new byte[64 * 1024];
            int bytesRead;
            while((bytesRead = fis.read(buffer))!=-1){
                md.update(buffer, 0, bytesRead);
            }
        }

        StringBuilder hexString = new StringBuilder();
        for(byte b: md.digest()){
            hexString.append(String.format("%02x", b));
        }

        return hexString.toString();
    }
}
