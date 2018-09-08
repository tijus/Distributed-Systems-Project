package edu.buffalo.cse.cse486586.simpledht;

import android.content.ContentValues;
import android.content.Context;
import android.content.SharedPreferences;
import android.database.Cursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.os.Bundle;
import android.app.Activity;
import android.provider.ContactsContract;
import android.telephony.TelephonyManager;
import android.text.method.ScrollingMovementMethod;
import android.util.Log;
import android.view.Menu;
import android.view.View;
import android.widget.Button;
import android.widget.EditText;
import android.widget.TextView;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

public class SimpleDhtActivity extends Activity implements View.OnClickListener {

    ArrayList<String> remotePort = new ArrayList<String>();
    TextView tv;
    int SERVER_PORT = 10000;
    HashMap<String, String> portTable = new HashMap<String, String>();
    HashMap<String, String> hashTable = new HashMap<String, String>();
    HashMap<String, String> successor = new HashMap<String, String>();
    HashMap<String, String> predecessor = new HashMap<String, String>();
    List<String> getValueSet;
    public static String keyToInsert,valueToInsert;
    final static String JOIN_REQ = "joinRequest";
    final static String JOIN_RES = "joinResponse";
    final static String QUERY_POS = "queryPosition";
    final static String QUERY_SINGLE = "querySingle";
    final static String MASTER_PORT = "11108";
    final static String FORWARD_MESSAGE = "forwardMessage";
    final static String QUERY_ALL = "queryAll";
    String myPort=null;

    Button lDump,gDump;
    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }
    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_simple_dht_main);
        tv = (TextView) findViewById(R.id.textView1);
        tv.setMovementMethod(new ScrollingMovementMethod());
        findViewById(R.id.button3).setOnClickListener(
                new OnTestClickListener(tv, getContentResolver()));

        Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");


        lDump = (Button)findViewById(R.id.button1);
        gDump = (Button)findViewById(R.id.button2);

        lDump.setOnClickListener(this);
        gDump.setOnClickListener(this);

        TelephonyManager tel = (TelephonyManager) this.getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = String.valueOf((Integer.parseInt(portStr) * 2));
        Log.e("PA3myPort", myPort);

        try {

            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            // Log.e(TAG, "Server listening to the port "+ SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);

        } catch (IOException e) {

            Log.e("Exception", "Can't create a ServerSocket");
            return;
        }

        // The joining node will send its port to the Master Node
        AsyncTask<String, Void, String> joining = new ClientTask();
        joining.executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, myPort, MASTER_PORT,JOIN_REQ);
    }

    @Override
    public void onClick(View v) {
        Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");
        switch (v.getId()){
            case R.id.button1:
                Cursor getLocal = getContentResolver().query(mUri, null, "@", null,null);
                if(getLocal!=null && getLocal.getCount()>0)
                {
                    while (getLocal.moveToNext())
                    {
                        int keyIndex = getLocal.getColumnIndex("key");
                        int valueIndex = getLocal.getColumnIndex("value");
                        String key = getLocal.getString(keyIndex);
                        String value = getLocal.getString(valueIndex);

                        tv.append(key +":"+ value+"\t\n");
                    }
                }
                break;
            case R.id.button2:
                Cursor getGlobal = getContentResolver().query(mUri, null, "*", null,null);
                if(getGlobal!=null && getGlobal.getCount()>0)
                {
                    while (getGlobal.moveToNext())
                    {
                        int keyIndex = getGlobal.getColumnIndex("key");
                        int valueIndex = getGlobal.getColumnIndex("value");
                        String key = getGlobal.getString(keyIndex);
                        String value = getGlobal.getString(valueIndex);

                        tv.append(key +":"+ value+"\t\n");
                    }
                }
                break;
        }

    }


    public class ServerTask extends AsyncTask<ServerSocket, String, Void> {


        @Override

        protected Void doInBackground(ServerSocket... sockets) {

            Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");
            ServerSocket serverSocket = sockets[0];

            Socket socket=null;
            String received_message="";
            try {
                while (true) {
                    socket = serverSocket.accept();

                    DataInputStream dataInputStream = new DataInputStream(socket.getInputStream());
                    received_message = dataInputStream.readUTF();
                    String []received_packet = splitPacket(received_message,"_");
                    String message = received_packet[0]; // Port address of the node that sent the request
                    String flag = received_packet[1]; // FLAG for identification
                    // This will appear on 5554
                    Log.e("PA3ActivityMessage", message);
                    Log.e("PA3ActivityFlagS", flag);
                    if(flag.equals(JOIN_REQ)) {

                        // 5554 receives joining request
                        Log.e("PA3ActivtiyMessage", message);
                        String desiredPort = message;
                        remotePort.add(desiredPort);
                        SimpleDhtProvider dhtProvider = new SimpleDhtProvider();
                        portTable = dhtProvider.genPortTable();
                        hashTable = dhtProvider.genHashTable(remotePort);
                        String hashPort = hashTable.get(desiredPort);
                        Log.e("PA3testshashPort", hashPort);
                        getValueSet = new ArrayList<String>(hashTable.values());
                        Collections.sort(getValueSet);
                        String successorValue = dhtProvider.getSuccessor(getValueSet, hashPort);
                        String predecessorValue = dhtProvider.getPredecessor(getValueSet, hashPort);

                        /* calculating successor and predecessor accross every node in the chord ring
                        ones the new node joins*/
                        if(!successor.containsKey(successorValue))
                        {
                            successor.put(hashPort,successorValue);
                        }
                        else
                        {
                            predecessor.put(successorValue,hashPort);
                            successor.put(hashPort,successorValue);
                        }
                        if(!predecessor.containsKey(predecessorValue))
                        {
                            predecessor.put(hashPort,predecessorValue);
                        }
                        else {
                            successor.put(predecessorValue,hashPort);
                            predecessor.put(hashPort,predecessorValue);
                        }
                        Log.e("PA3ProviderRequest", predecessorValue+":"+successorValue);
                        // Calculate the number of joining nodes
                        SimpleDhtProvider.number_joins = SimpleDhtProvider.number_joins +1;
                        Log.e("joins", String.valueOf(SimpleDhtProvider.number_joins));
                        // send the successors and predecessors to the desired port
                        AsyncTask<String, Void, String> result = new ClientTask();
                        result.executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, MASTER_PORT,desiredPort, JOIN_RES);
                    }
                    else if(flag.equals(JOIN_RES))
                    {
                        // any node receives joining response from 5554 his entry has been made
                        if(message.equals("11108"))
                        {
                            SharedPreferences preferences = getSharedPreferences("MasterAlive",Context.MODE_PRIVATE);
                            SharedPreferences.Editor editor = preferences.edit();
                            editor.putBoolean("status",true);
                            editor.commit();
                        }
                    }
                    else if(flag.equals(QUERY_POS))
                    {
                        // Handle the successor and predecessor maintained at 5554
                        /* Any node queries 5554 to obtain it's successor and predecessor along with
                         * the node with the minimum hash value */

                        /*
                            message: contains successor and predecessor
                            FLAG: for identification
                         */
                        SimpleDhtProvider dhtProvider = new SimpleDhtProvider();
                        portTable = dhtProvider.genPortTable();
                        hashTable = dhtProvider.genHashTable(remotePort);
                        String hashPort = hashTable.get(message);
                        Log.e("PA3testsMessage",hashPort);
                        String predec = predecessor.get(hashPort);
                        String success = successor.get(hashPort);
                        String minNodeHash = Collections.min(predecessor.keySet());
                        Log.e("PA3Providerpresuc",message+":"+hashPort+" : "+predec+" : "+success+":"+minNodeHash);
                        String messageToSend = MASTER_PORT+"_"+predec+"_"+success+"_"+minNodeHash;
                        DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
                        dataOutputStream.writeUTF(messageToSend);
                    }
                    else if(flag.equals(FORWARD_MESSAGE))
                    {
                        // This section is triggered due the forwarding of the message during insert
                        /*
                            message: KeyValue pair obtained from the predecessor
                            flag: Used for identification
                         */
                        String[] keyValuePair = splitPacket(message, "&");
                        String key = keyValuePair[0]; // Key to be forwarded
                        String value = keyValuePair[1]; // value to be forwarded
                        String forwardPort = keyValuePair[2]; // forwarded to this node
                        String minNodeHash = keyValuePair[3]; // node with the minimum hash value
                        Log.e("PA3ForwardMessageLog", key+":"+value+":"+forwardPort);

                        Log.e("minNodeHash",minNodeHash);

                        /* Stores the message to the with the minimum hash value if its storage is not found*/
                        if(forwardPort.equals(minNodeHash))
                        {

                            if(key.contains("$"))
                            {
                                String refactoredKey = key.substring(0, key.length()-1);
                                SimpleDhtProvider dhtProvider = new SimpleDhtProvider();
                                String hashRefactoredKey = dhtProvider.genHash(refactoredKey);
                                /* Forwards the message if predecessor is greater then the message or if
                                * message can be stored to the more optimal node than the asked node */
                                if(hashRefactoredKey.compareTo(minNodeHash)<0)
                                {
                                    key = refactoredKey;
                                    Log.e("PA3Forward&StoreSpecial", key+":"+value+":"+forwardPort);
                                    SharedPreferences sharedPreferences = getSharedPreferences("StoredKeyValue", Context.MODE_PRIVATE);
                                    SharedPreferences.Editor editor = sharedPreferences.edit();
                                    editor.putString(key,value);
                                    editor.commit();
                                }
                                else
                                {
                                    ContentValues contentValues = new ContentValues();
                                    contentValues.put("key",key);
                                    contentValues.put("value",value);
                                    getContentResolver().insert(mUri,contentValues);
                                }
                            }
                            // Store the message if the message hash is smaller than the hash of the minimum node
                            else
                            {
                                Log.e("PA3Forward&Store", key+":"+value+":"+forwardPort);
                                SharedPreferences sharedPreferences = getSharedPreferences("StoredKeyValue", Context.MODE_PRIVATE);
                                SharedPreferences.Editor editor = sharedPreferences.edit();
                                editor.putString(key,value);
                                editor.commit();
                            }

                        }
                        // Recursively call insert if none of the above is satisfied
                        else
                        {
                            ContentValues contentValues = new ContentValues();
                            contentValues.put("key",key);
                            contentValues.put("value",value);
                            getContentResolver().insert(mUri,contentValues);
                        }
                    }
                    else if(flag.equals(QUERY_SINGLE))
                    {
                        /*
                        *  This section is triggered when the message asked for to one node resides on the other node
                        *  message:
                        *   port: the node to whom message was queried for the first time
                        *   sendKey: The key of the message
                         */
                        String[] receivedPacket = splitPacket(message, "&");
                        String port = receivedPacket[0];
                        String sentKey = receivedPacket[1];
                        sentKey = sentKey + "_" + port;
                        //Log.e("foundKey", port + ":" + sentKey);
                        Cursor queryCursor = getContentResolver().query(mUri, null, sentKey, null, null);
                        Log.e("queryCursor", String.valueOf(queryCursor==null));
                        // If message is found on the node than return
                        if(queryCursor!=null && queryCursor.moveToFirst())
                        {
                            int keyIndex = queryCursor.getColumnIndex("key");
                            int valueIndex = queryCursor.getColumnIndex("value");
                            Log.e("foundKeyValue", keyIndex+":"+valueIndex);
                            String key = queryCursor.getString(keyIndex);
                            String value = queryCursor.getString(valueIndex);
                            String sendKeyValue = key+"_"+value;

                            Log.e("sendfound", sendKeyValue);
                            DataOutputStream outputStream = new DataOutputStream(socket.getOutputStream());
                            outputStream.writeUTF(sendKeyValue);
                            //Log.e("foundKeyValuev", key+":"+value);
                        }
                    }
                    else if(flag.equals(QUERY_ALL)){
                        /*
                         *  This section is triggered when the global dump of the message is asked
                         *  message:
                         *   port: the node to whom message was queried for the first time
                         *   sendKey: The key of the message
                         */

                        SimpleDhtProvider dhtProvider = new SimpleDhtProvider();
                        portTable = dhtProvider.genPortTable();
                        hashTable = dhtProvider.genHashTable(SimpleDhtProvider.remotePort);
                        //Log.e("remotePortSize",String.valueOf(remotePort.size()));
                        String hashPort = hashTable.get(message);
                        Log.e("testSuccessor", message+":"+hashPort+":"+SimpleDhtProvider.successor);
                        String sendingResult="";

                        // The pointer to the node will stop it reaches the predecessor to the node
                        if(hashPort.equals(SimpleDhtProvider.successor))
                        {
                            Log.e("queryPositionAll", "optimal reached");
                            Cursor queryLocal = getContentResolver().query(mUri, null, "@", null, null);
                            Log.e("checkQuery",String.valueOf(queryLocal.getCount()));
                            // if the return cursor is not null then it will extract the local dump and forward it to the socket
                            if(queryLocal!=null && queryLocal.getCount()>0)
                            {
                                while(queryLocal.moveToNext())
                                {
                                    int keyIndex = queryLocal.getColumnIndex("key");
                                    int valueIndex = queryLocal.getColumnIndex("value");
                                    String key = queryLocal.getString(keyIndex);
                                    String value = queryLocal.getString(valueIndex);
                                    Log.e("fetchLocal", key+","+value);
                                    sendingResult = key+"&"+value+":"+sendingResult;
                                }
                            }
                            else
                            {
                                sendingResult = "null&null";
                            }
                            Log.e("sendingLocal", sendingResult);
                            DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
                            dataOutputStream.writeUTF(sendingResult);
                        }
                        else
                        {
                            // Will extract local dumps from each node recursivesly
                            String sentKey = "Star_"+message;
                            Cursor queryCursor = getContentResolver().query(mUri, null, sentKey, null, null);
                            Log.e("checkQueryCursor",String.valueOf(queryCursor.getCount()));
                            if(queryCursor!=null && queryCursor.getCount()>0)
                            {
                                while(queryCursor.moveToNext())
                                {
                                    int keyIndex = queryCursor.getColumnIndex("key");
                                    int valueIndex = queryCursor.getColumnIndex("value");
                                    String key = queryCursor.getString(keyIndex);
                                    String value = queryCursor.getString(valueIndex);
                                    sendingResult = key+"&"+value+":"+sendingResult;
                                }
                            }
                            else
                            {
                                sendingResult = "null&null";
                            }
                            Log.e("sendingLocal", sendingResult);
                            DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
                            dataOutputStream.writeUTF(sendingResult);
                        }
                    }
                }
            }
            catch (IOException e) {
                e.printStackTrace();
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            } finally {
                // close the socket after use
                try {
                    socket.close();
                    // Closing the server socket after use
                    serverSocket.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            return null;
        }
    }

    public static class ClientTask extends AsyncTask<String, Void, String> {

        // Client side code
        @Override
        protected String doInBackground(String... msgs) {

            try {
                if(msgs[2].equals(JOIN_REQ)) {

                    // send joining request from any node
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(msgs[1]));

                    String msgToSend = msgs[0];

                    DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
                    String packet = createPacket(msgToSend,JOIN_REQ);
                    dataOutputStream.writeUTF(packet);
                    Log.e("PA3sendMessage", msgToSend + ": " + msgs[1]);
                    return "yes";
                }
                else if(msgs[2].equals(JOIN_RES))
                {
                    // send joining response form 5554
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10,0,2,2}),
                            Integer.parseInt(msgs[1]));
                    String msgToSend = msgs[0];

                    DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
                    String packet = createPacket(msgToSend,JOIN_RES);
                    dataOutputStream.writeUTF(packet);
                    return "yes";

                }
                else if(msgs[2].equals(QUERY_POS))
                {
                    // any node sends query request about successor and predecessor to 5554
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(msgs[1]));

                    String msgToSend = msgs[0];


                    DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
                    String packet = createPacket(msgToSend,QUERY_POS);
                    dataOutputStream.writeUTF(packet);
                    DataInputStream dataInputStream = new DataInputStream(socket.getInputStream());
                    String messageReceived = dataInputStream.readUTF();
                    String[] messagePacket = splitPacket(messageReceived,"_");
                    Log.e("PA3sendMessageR", msgToSend + ": " + msgs[1]);
                    Log.e("Test",messagePacket[1]+"_"+messagePacket[2]);
                    return messagePacket[1]+"_"+messagePacket[2]+"_"+messagePacket[3];
                }
                else if(msgs[2].equals(FORWARD_MESSAGE))
                {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(msgs[1]));

                    String []msgPacket = splitPacket(msgs[0], "_");
                    String msgToSend = msgPacket[0]+"&"+msgPacket[1]+"&"+msgPacket[2]+"&"+msgPacket[3];
                    DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
                    String packet = createPacket(msgToSend,FORWARD_MESSAGE);
                    dataOutputStream.writeUTF(packet);
                    Log.e("PA3sendMessageFR", msgToSend + ": " + msgs[1]);
                    return "yes";

                }
                else if(msgs[2].equals(QUERY_SINGLE))
                {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(msgs[1]));
                    String msgPacket = msgs[0];
                    DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
                    String packet = createPacket(msgPacket, msgs[2]);
                    Log.e("PA3sendMessageQS", packet + ": " + msgs[1]);
                    dataOutputStream.writeUTF(packet);
                    DataInputStream inputStream = new DataInputStream(socket.getInputStream());
                    String receivedKeyPair = inputStream.readUTF();
                    Log.e("socketRead", receivedKeyPair);
                    return receivedKeyPair;
                }
                else if(msgs[2].equals(QUERY_ALL))
                {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(msgs[1]));
                    String msgPacket = msgs[0];
                    DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
                    String packet = createPacket(msgPacket, msgs[2]);
                    Log.e("PA3sendMessageQA", packet + ": " + msgs[1]);
                    dataOutputStream.writeUTF(packet);
                    DataInputStream inputStream = new DataInputStream(socket.getInputStream());
                    String receivedKeyPair = inputStream.readUTF();
                    Log.e("socketReadA", receivedKeyPair);
                    return receivedKeyPair;
                }


            } catch (UnknownHostException e) {
                Log.e("PA3Activity", "ClientTask UnknownHostException");
            } catch (IOException e) {
                Log.e("PA3Activity", "ClientTask socket IOException");
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
            return null;
        }
    }

    // create packet to send
    protected static String createPacket(String msgToSend, String FLAG) throws NoSuchAlgorithmException {

        return msgToSend+"_"+FLAG;

    }

    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.activity_simple_dht_main, menu);
        return true;
    }

    // Split packet
    public static String[] splitPacket(String packet, String splitKey) {
        String[] packet_parts = packet.split(splitKey);
        return packet_parts;
    }

}