package dfs.common;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class ConfigManager {
    private static final Properties properties = new Properties();

    static {
        try(InputStream input = new FileInputStream("dfs.properties")){
            properties.load(input);
            System.out.println("Configuration loaded from dfs.properties");
        }catch (IOException e){
            System.err.println("Failed to load dfs.properties. Falling back to defaults");
        }
    }

    public static String getString(String key, String defaultValue){
        return properties.getProperty(key, defaultValue);
    }

    public static int getInt(String key, int defaultValue){
        String value = properties.getProperty(key);
        if(value!=null){
            try {
                return Integer.parseInt(value);
            }catch (NumberFormatException e){
                System.err.println("Warning: invalid integer for "+key+". Using default: "+defaultValue);
            }
        }
        return defaultValue;
    }
}
