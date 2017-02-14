import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by rittick on 2/4/17.
 */
public class Peer {
    //After reading config file, these fields will be initialized.
    private static String ip;
    private static String port;
    private static Integer numberOfServers;

    // This will store the key-value pairs.
    private static ConcurrentHashMap<String, String> register = new ConcurrentHashMap<>();

    //Streams to read user input
    private static BufferedReader userInput = new BufferedReader(new InputStreamReader(System.in));

    //Variable for user choice
    private static String serverID;

    //Config file data is stored here.
    private static ConcurrentHashMap<Integer, String> configFileData;

    public static void main(String[] args) {
        //-------------------------Getting Server ID----------------------------------------
        System.out.println("Enter Server ID number (e.g 1,2,3,...,8): ");
        try {
            serverID = userInput.readLine();
        } catch (IOException e) {
            System.out.println("IO Exception Occurred. PEER MAIN, @ ServerID Input");
        }

        //-------Getting IP Address, Port and Number of Servers From Config File------------
        configFileData = ReadConfigFile.readFile();

        /*for(Integer key : configFileData.keySet()){
            System.out.println(key+ " : "+ configFileData.get(key));
        }*/

        String ipAndPort = configFileData.get(Integer.parseInt(serverID));

        parseServerInfo(ipAndPort);


        //-----------------------STARTING PEER SERVER---------------------------------------
        try {
            ServerSocket peerServer = new ServerSocket(Integer.parseInt(port));
            System.out.println("Peer Server is Listening...");
            //System.out.println("# CHECKPOINT");
            //------------------------STARTING PEER CLIENT--------------------------------------
            new Client(ip, port, numberOfServers).start();
            System.out.println("Peer Client is Running....");

            while (true) {
                Socket newConnection = peerServer.accept();
                new Server(serverID, newConnection).start();
            }


        } catch (IOException e) {
            System.out.println("IO Exception Occurred. After Starting Peer Client");
        }
    }

    /***
     *  This class listens for client connections and handles PUT, GET and DEL requests.
     */
    private static class Server extends Thread {

        private String serverID;
        private Socket connection;
        private BufferedReader incomingStream;
        private PrintWriter outgoingStream;

        public Server(String serverID, Socket newConnection) {
            this.serverID = serverID;
            this.connection = newConnection;
        }

        public void run() {
            //System.out.println("In run() of Server.");
            try {
                incomingStream = new BufferedReader(new InputStreamReader(connection.getInputStream()));
                outgoingStream = new PrintWriter(connection.getOutputStream());
                //System.out.println("# SERVER CHECKPOINT");

                while (true) {
                    String clientsMessage = incomingStream.readLine();
                    String[] clientsSplitMessage = clientsMessage.split(":"); //Check NullPointerException
                    String clientRequestID = clientsSplitMessage[0];

                    switch (clientRequestID) {
                        case "1":
                            System.out.println("-----SERVER LOG: Client wants to PUT-----");
                            System.out.println("Key: " + clientsSplitMessage[1]);
                            System.out.println("Value: " + clientsSplitMessage[2]);

                            if (put(clientsSplitMessage[1], clientsSplitMessage[2])) {
                                System.out.println("Key-Value Pair Successfully Added!");
                                //Client.selectOption();
                            } else {
                                System.out.println("Put Operation Failed.");
                            }

                            //Client.selectOption();
                            try {
                                outgoingStream.println("Key-Value Pair Successfully Added!");
                                outgoingStream.close();
                            } catch (Exception e) {
                                System.out.println("EXCEPTION, SERVER, CASE 1 ");
                            }
                            Options();
                            break;

                        case "2":
                            System.out.println("-----SERVER LOG: Client wants to GET-----");
                            System.out.println("Key: "+ clientsSplitMessage[1]);

                            String value_from_register = get(clientsSplitMessage[1]);
                            //System.out.println(value_from_register);
                            if(value_from_register == null){
                                outgoingStream.println("VALUE NOT FOUND.");
                                outgoingStream.close();
                                Options();
                                break;
                            } else{
                                outgoingStream.println("Value: "+value_from_register);
                                outgoingStream.close();
                                Options();
                                break;
                            }



                        case "3":
                            System.out.println("-----SERVER LOG: Client wants to DEL-----");
                            System.out.println("Key: "+ clientsSplitMessage[1]);
                            boolean delStatus = del(clientsSplitMessage[1]);

                            if(delStatus){
                                System.out.println("Key-Value Pair Successfully Deleted!");
                                outgoingStream.println(delStatus);
                                outgoingStream.close();
                                Options();
                                break;
                            }else{
                                outgoingStream.println(delStatus);
                                outgoingStream.close();
                                Options();
                                break;
                            }

                            //Client.selectOption();
                            //break;

                        default:
                            System.out.println("No Input From Client.");

                    }
                }


                //System.out.println("Client's Message: " + clientsMessage);

            } catch (IOException e) {
                //System.out.println("IO Exception Occurred. Server's RUN METHOD");
            }
        }

        /***
         * This method takes key-value pair and inserts into "register". It returns a boolean value indicating
         * success or failure of the operation. (true -> SUCCESS & false -> FAILURE)
         * @param key
         * @param value
         * @return boolean
         */
        public boolean put(String key, String value) {
            boolean flag = false;

            register.put(key.trim(), value.trim());
            if (register.get(key.trim()) != null) {
                flag = true;
            }
            //System.out.println("IN PUT: "+get(key));
            return flag;
        }

        /***
         * This method takes a key and returns the value. It searches "register".
         * @param key
         * @return
         */
        public String get(String key) { //TODO register key type and argument type not matching. FIX IT.

            for(Map.Entry<String, String> entry : register.entrySet()){
                if(entry.getKey().equals(key)){
                    //System.out.println("Key Present.");
                }else{
                    //System.out.println("Key Absent.");
                }

            }

            String value = register.get(key);
            if (value != null) {
                System.out.println("IN GET: CHECKING NULL");
                return value;
            }
            System.out.println("Value: "+value);
            return value;
        }

        /***
         * This method removes the key-value pair from the "register". If after removal, get(key) returns null,
         * deletion operation is successful and flag is set to true.
         * @param key
         * @return
         */
        public boolean del(String key) {
            boolean flag = false;

            register.remove(key);
            if (register.get(key) == null) {
                flag = true;
            }

            return flag;
        }


        private static void Options() throws IOException {
            System.out.println("---------SELECT AN OPERATION-----------");
            System.out.println("1.  PUT    (Key-Value Pair) ");
            System.out.println("2.  GET    (Value of a Key)");
            System.out.println("3.  DEL    (A Key-Value Mapping from Register");
        }
    }

    /***
     * This class connects to server based on hash of key and makes request to
     * 1. PUT
     * 2. GET
     * 3. DEL
     * THERE IS CONSISTENT HASHING OF KEY. THIS CLASS USES CLASS: "KeyHash"
     */
    private static class Client extends Thread {
        private String IP_ADDRESS;
        private Integer PORT;
        private Integer NUMBER_OF_SERVERS;

        //Variables for Socket Communication.
        Socket socket = null;
        BufferedReader keyboardInput = null;
        BufferedReader streamInput = null;
        PrintWriter printWriter = null;

        //Variables for PUT, GET and DEL Operations
        private String key;
        private String value;
        private String getResult;
        private String delResult;

        public Client(String ip, String port, Integer numberOfServers) {
            this.IP_ADDRESS = ip;
            this.PORT = Integer.parseInt(port);
            this.NUMBER_OF_SERVERS = numberOfServers;
        }

        /***
         * This method connects to Server and makes requests(PUT, GET, AND DEL.)
         */
        public void run() {
            //System.out.println("In run() of Client.");
            try {
                // Initializing socket, readers and writers. //TODO Remove these general socket, reader and writer objects.
                socket = new Socket(IP_ADDRESS, PORT);
                keyboardInput = new BufferedReader(new InputStreamReader(System.in));
                streamInput = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                printWriter = new PrintWriter(socket.getOutputStream());

                // User Option Selection
                //System.out.println("# CLIENT CHECKPOINT");


                while (true) {
                    Integer userSelection = selectOption();
                    //System.out.println(userSelection);

                    switch (userSelection) {
                        case 1:
                            String IP;
                            Integer PORT;
                            System.out.println("-----------PUT Request------------");
                            System.out.println("Enter Key: (MAX of 12 Characters)"); //------------GET KEY-----------
                            key = keyboardInput.readLine();
                            while (key.length() > 12) {
                                System.out.println("Key Length Exceeding 12 Characters.");
                                System.out.println("Enter Key: (MAX of 12 Characters)");
                                key = keyboardInput.readLine();
                            }

                            System.out.println("Enter Value: (MAX of 500 Characters)"); //---------GET VALUE---------
                            value = keyboardInput.readLine();
                            while (value.length() > 500) {
                                System.out.println("Value Length Exceeding 500 Characters.");
                                System.out.println("Enter Value: (MAX of 500 Characters)");
                                value = keyboardInput.readLine();
                            }

                            System.out.println("Key: " + key);
                            System.out.println("Value: " + value);

                            int keyHash = KeyHash.getKeyHash(key, numberOfServers); //-------------GET HASHCODE------
                            System.out.println("HashCode: " + keyHash);
                            String serverInfo = configFileData.get(keyHash);
                            IP = serverInfo.split(":")[0];
                            PORT = Integer.parseInt(serverInfo.split(":")[1]);
                            System.out.println("Server IP: " + IP);
                            System.out.println("Server PORT: " + PORT);

                            if (this.IP_ADDRESS.equals(IP) && this.PORT.equals(PORT)) {
                                System.out.println("Note: Server belongs to this Peer.");
                                register.put(key, value);
                                System.out.println("Key-Value Pair Successfully Added!");
                            } else {
                                System.out.println("Connecting to server...");
                                socket = new Socket(IP, PORT);
                                streamInput = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                                printWriter = new PrintWriter(socket.getOutputStream());
                                printWriter.println("1:" + key + " : " + value);
                                printWriter.flush();

                                //String serversMessage = streamInput.readLine();
                                //System.out.println("Server's Message: "+serversMessage);
                                //break;
                            }

                            break;

                        case 2:
                            String IP_GET;
                            Integer PORT_GET;
                            System.out.println("----------------GET Request----------------");
                            System.out.println("Enter Key: (MAX of 12 Characters)"); //------------GET KEY-----------
                            key = keyboardInput.readLine();
                            while (key.length() > 12) {
                                System.out.println("Key Length Exceeding 12 Characters.");
                                System.out.println("Enter Key: (MAX of 12 Characters)");
                                key = keyboardInput.readLine();
                            }
                            System.out.println("Key: " + key);

                            int keyHash_GET = KeyHash.getKeyHash(key, numberOfServers); //-------------GET HASHCODE------
                            System.out.println("HashCode: " + keyHash_GET);
                            String serverInfo_GET = configFileData.get(keyHash_GET);
                            IP_GET = serverInfo_GET.split(":")[0];
                            PORT_GET = Integer.parseInt(serverInfo_GET.split(":")[1]);
                            System.out.println("Server IP: " + IP_GET);
                            System.out.println("Server PORT: " + PORT_GET);

                            if (this.IP_ADDRESS.equals(IP_GET) && this.PORT.equals(PORT_GET)) {
                                System.out.println("Note: Server belongs to this Peer.");
                                String value_from_register = register.get(key);
                                System.out.println("Value: "+value_from_register);

                                if(value_from_register != null) {
                                    System.out.println("Value Successfully Retrieved!");
                                }else{
                                    System.out.println("Value Not Found.");
                                }

                            }else {
                                System.out.println("Connecting to server...");
                                socket = new Socket(IP_GET, PORT_GET);
                                streamInput = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                                printWriter = new PrintWriter(socket.getOutputStream());
                                printWriter.println("2:" + key);
                                printWriter.flush();
                                //TODO Read Server Input and display Value.
                                String getResult = streamInput.readLine();
                                System.out.println("Value: "+getResult);

                                //break;
                            }
                            break;

                        case 3:
                            String IP_DEL;
                            Integer PORT_DEL;
                            System.out.println("----------------DEL Request----------------");
                            System.out.println("Enter Key: (MAX of 12 Characters)"); //------------DEL KEY-----------
                            key = keyboardInput.readLine();
                            while (key.length() > 12) {
                                System.out.println("Key Length Exceeding 12 Characters.");
                                System.out.println("Enter Key: (MAX of 12 Characters)");
                                key = keyboardInput.readLine();
                            }
                            System.out.println("Key: " + key);

                            int keyHash_DEL = KeyHash.getKeyHash(key, numberOfServers); //-------------GET HASHCODE------
                            System.out.println("HashCode: " + keyHash_DEL);
                            String serverInfo_DEL = configFileData.get(keyHash_DEL);
                            IP_DEL = serverInfo_DEL.split(":")[0];
                            PORT_DEL = Integer.parseInt(serverInfo_DEL.split(":")[1]);
                            System.out.println("Server IP: " + IP_DEL);
                            System.out.println("Server PORT: " + PORT_DEL);

                            if (this.IP_ADDRESS.equals(IP_DEL) && this.PORT.equals(PORT_DEL)) {
                                System.out.println("Note: Server belongs to this Peer.");
                                String removedItem = register.remove(key);// TODO
                                System.out.println("Deleted Value: "+removedItem);

                                if(removedItem != null) {
                                    System.out.println("Key-Value Pair Successfully Deleted!");
                                }else{
                                    System.out.println("Value Not Found.");
                                }

                            }else {
                                System.out.println("Connecting to server...");
                                socket = new Socket(IP_DEL, PORT_DEL);
                                streamInput = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                                printWriter = new PrintWriter(socket.getOutputStream());
                                printWriter.println("3:" + key);
                                printWriter.flush();
                                //TODO Read Server Input and display Value.
                                String getResult = streamInput.readLine();
                                System.out.println("Key-Value Pair Successfully Deleted: "+getResult);

                                break;
                            }
                    }
                }


            } catch (IOException e) {
                System.out.println("IO Exception Occurred. Client's RUN Method.");
            }

        }

        private static Integer selectOption() throws IOException {
            System.out.println("---------SELECT AN OPERATION-----------");
            System.out.println("1.  PUT    (Key-Value Pair) ");
            System.out.println("2.  GET    (Value of a Key)");
            System.out.println("3.  DEL    (A Key-Value Mapping from Register");
            String userSelection = userInput.readLine();
            return Integer.parseInt(userSelection);
        }

    }


    //------------------------HELPER METHODS-------------------------------------------

    /**
     * This method parses a string and extracts IP Address, Port and Number of Servers.
     *
     * @param ipAndPort
     */
    private static void parseServerInfo(String ipAndPort) {

        String[] split = ipAndPort.split(":");
        ip = split[0];
        port = split[1];
        numberOfServers = configFileData.keySet().size();
        //System.out.println(ip + " " + port + " " + numberOfServers);
    }
}
