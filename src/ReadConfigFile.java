/**
 * Created by rittick on 2/4/17.
 */

import java.io.*;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/***
 * This class will read the config file and return appropriate value containing IP and port number of server.
 */
public class ReadConfigFile {

    public static ConcurrentHashMap<Integer, String> readFile(/*String serverNumber*/) {

        String serverAndPortAndSize = null;
        FileInputStream fileInputStream = null;
        int size = 0;
        ConcurrentHashMap<Integer, String> configFileData = new ConcurrentHashMap<>();

        try {
            fileInputStream = new FileInputStream("src/servers.config");
        } catch (FileNotFoundException e) {
            System.out.println("File Not Found.");
        }

        Properties properties = new Properties();
        try {

            properties.load(fileInputStream);
            Set<String> strings = properties.stringPropertyNames();
            size = strings.size();
            for(String key: strings){
                configFileData.put(Integer.parseInt(key), properties.getProperty(key));
            }

        } catch (IOException e) {
            System.out.println("IO Exception.");
        }

        //String property = properties.getProperty(serverNumber);

        //serverAndPortAndSize = property+":"+size;
        return configFileData;
    }

   /* public static void main(String[] args) throws IOException {

        String serverAndPort = readFile("1");
        String[] split = serverAndPort.split(":");

        System.out.println("IP: "+split[0]);
        System.out.println("Port: "+split[1]);
        System.out.println("Number of Nodes: "+ split[2]);
    }*/
}
