package edu.buffalo.cse.cse486586.simpledht;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.content.SharedPreferences;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDhtProvider extends ContentProvider {

    public static ArrayList<String> remotePort= new ArrayList<String>(Arrays.asList("11108", "11112","11116","11120","11124"));
    HashMap<String, String> portTable = new HashMap<String, String>();
    HashMap<String, String> hashTable = new HashMap<String, String>();
    final static String QUERY_POS = "queryPosition";
    final static String FORWARD_MESSAGE = "forwardMessage";
    final static String QUERY_SINGLE = "querySingle";
    public static String successor, predecessor,minNodeHash=null;
    final static String QUERY_ALL = "queryAll";
    public static Integer number_joins = 0;

    public int delete(Uri uri, String selection, String[] selectionArgs) {

        // Handles delete with selectiom parameter as a key of the message

        /*
        Ref: https://developer.android.com/reference/android/content/SharedPreferences.Editor.html
        */

        SharedPreferences sharedPreferences = getContext().getSharedPreferences("StoredKeyValue", Context.MODE_PRIVATE);
        SharedPreferences.Editor editor = sharedPreferences.edit();

        String key = selection;
        editor.remove(key);
        editor.apply();
        if(!editor.commit())
            return 0;
        return 1;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {

        // TODO Auto-generated method stub
        /* Creating shared preferences (storage system for android)
            with key value pair
            ref: https://developer.android.com/reference/android/content/SharedPreferences.html
         */

        String key = values.get("key").toString();
        String value = values.get("value").toString();

        if(key.contains("$"))
        {
            key = key.substring(0, key.length()-1);

        }

        Log.e("PA3keytest",key);
        portTable = genPortTable();
        String myPort = getSystemPort();
        Log.v("PA3portreachability",myPort);

        // hash generation
        String hashPort = null;
        String hashKey=null;
        try {
            hashPort = genHash(portTable.get(myPort));
            hashKey= genHash(key);
            hashTable = genHashTable(remotePort);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        SharedPreferences sharedPreferences = getContext().getSharedPreferences("StoredKeyValue", Context.MODE_PRIVATE);
        SharedPreferences.Editor editor = sharedPreferences.edit();

        // Shared preference to check if master node is alive
        SharedPreferences checkMaster = getContext().getSharedPreferences("MasterAlive", Context.MODE_PRIVATE);
        Boolean status = checkMaster.getBoolean("status",false);

            Log.e("ProviderJoin",String.valueOf(status == true && !number_joins.equals(1)));
            // if multiple avds are alive
            if(status == true && !number_joins.equals(1)) {
                Log.e("status", status.toString()+":"+key);
                if(predecessor==null && successor==null) {
                    AsyncTask<String, Void, String> result = new SimpleDhtActivity.ClientTask();
                    String message = null;
                    try {
                        message = result.executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, myPort, "11108", QUERY_POS).get();
                        Log.e("messageFromActivity", message);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    } catch (ExecutionException e) {
                        e.printStackTrace();
                    }

                    String[] messagePacket = SimpleDhtActivity.splitPacket(message, "_");
                    predecessor = messagePacket[0];
                    successor = messagePacket[1];
                    minNodeHash = messagePacket[2];
                }
                Log.e("PA3TestKey", predecessor+":"+successor );

                Log.e("PA3PredecSuccess", value + " : " + hashPort + ":" + predecessor + ":" + successor);
                Log.e("PA3testss", String.valueOf(hashKey.compareTo(hashPort)>0)+":"+hashKey);

                // if the message node is greater then the node hash forward, else store
                if(hashKey.compareTo(hashPort)>0)
                {
                    Log.e("PA3testssMessage", "FORWARDMESSAGE to"+":"+key+" to:"+successor+"from: "+ hashPort+":"+minNodeHash);
                    Log.e("PA3successorkey", successor+":"+getKey(successor));

                    new SimpleDhtActivity.ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, key+"_"+value+"_"+successor+"_"+minNodeHash,getKey(successor), FORWARD_MESSAGE);
                }
                else
                {
                    // if the predecessor is greater than the message hash, then handle message
                    // through forwarding accross every node in the ring till it reaches its rightful node
                    if(hashKey.compareTo(predecessor)<0)
                    {
                        Log.e("PA3PredecGreat",  "FORWARDMESSAGESPECIAL"+":"+key+" to:"+successor+"from: "+ hashPort+":"+minNodeHash);
                        if(!key.contains("$"))
                            key = key+"$";
                        new SimpleDhtActivity.ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, key+"_"+value+"_"+successor+"_"+minNodeHash,getKey(successor), FORWARD_MESSAGE);
                    }
                    else
                    {
                        Log.e("PA3testssMessage", "STOREMESSAGE" + ":" + key);
                        Log.e("PA3Message", key);
                        editor.putString(key, value);
                        editor.commit();
                    }
                }
            }
        // If only one node is alive
        else
        {
            Log.v("PA3hashedPort", hashPort);

            // Perform Insert operation
            Log.v("PA3testInsert", key+" : "+value);

            editor.putString(key,value);
            editor.commit();
        }
        return uri;
    }

    // Caluclate successor of the given node
    public String getSuccessor(List<String> getValueSet, String hashPort) {
        String successor = null;
        int index = getValueSet.indexOf(hashPort);
        try {
            successor = getValueSet.get(index+1);
        }
        catch (IndexOutOfBoundsException e)
        {
            successor = getValueSet.get(0);
        }
        //successor[0] = getKey(successor[1]);
        return successor;
    }

    // Calculate predecessor of the given node
    public String getPredecessor(List<String> getValueSet, String hashPort) {
        String predecessor = null;
        int index = getValueSet.indexOf(hashPort);
        try {
            predecessor = getValueSet.get(index-1);
        }
        catch (ArrayIndexOutOfBoundsException e)
        {
            predecessor = getValueSet.get(getValueSet.size()-1);
        }
        //predecessor[0] = getKey(predecessor[1]);
        return predecessor;
    }

    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub
        return false;
    }

    // Consturct a mapping table of port no and avd's no.
    public HashMap<String,String> genPortTable()
    {
        HashMap<String, String> portTable = new HashMap<String, String>();
        portTable.put("11108","5554");
        portTable.put("11112","5556");
        portTable.put("11116","5558");
        portTable.put("11120","5560");
        portTable.put("11124","5562");

        return portTable;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {
        /*
        Getting the values stored in the shared preferences
         */

        String senderPort = "";


        SharedPreferences checkMaster = getContext().getSharedPreferences("MasterAlive", Context.MODE_PRIVATE);
        Boolean status = checkMaster.getBoolean("status",false);
        if(selection.contains("_"))
        {
            String[] selectionParam = SimpleDhtActivity.splitPacket(selection, "_");
            selection = selectionParam[0];
            senderPort = selectionParam[1];
        }
        Log.e("querySelection", selection);
        Log.e("queryStatus", status.toString());
        String myPort = getSystemPort();
        SharedPreferences sharedPreferences = getContext().getSharedPreferences("StoredKeyValue", Context.MODE_PRIVATE);
        String key = selection;
        String[] cols = new String[]{"key", "value"};
        MatrixCursor matrixCursor = new MatrixCursor(cols);
        Object[] rowValues = new Object[2];
        Log.e("querySelection", selection);
        Log.e("querySelectionStatus", String.valueOf(selection.equals("@")));
        Log.e("queryStatus", status.toString());
        if(selection.equals("@"))
        {
            // Show local data of the AVDs
            Map<String, ?> allKeyValue = sharedPreferences.getAll();
            for(Map.Entry<String,?> keyValue : allKeyValue.entrySet())
            {
                rowValues[0] = keyValue.getKey();
                rowValues[1] = keyValue.getValue().toString();
                matrixCursor.addRow(rowValues);
                Log.v("PA3allLocalVaues", "key : "+ keyValue.getKey() + " value: "+keyValue.getValue().toString());
            }
            return matrixCursor;
        }
        else if(selection.equals("*") || selection.equals("Star"))
        {
            if(senderPort.equals(""))
                senderPort = myPort;
            // If mulltiple avd's are alive
            if(status==true && !number_joins.equals(1))
            {
                String receiveQueryRequestAll="";
                try {
                    AsyncTask<String, Void, String> queryAll = new SimpleDhtActivity.ClientTask();
                    receiveQueryRequestAll = queryAll.executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, senderPort, getKey(successor), QUERY_ALL).get();
                    Log.e("receiveQueryRequestAll", receiveQueryRequestAll);

                    String []receiveQueryPair = SimpleDhtActivity.splitPacket(receiveQueryRequestAll,":");
                    for(String receiveQuery : receiveQueryPair)
                    {
                        // if the cursor return is not null or the avd is not empty
                        if(!receiveQuery.equals("null&null"))
                        {
                            Log.e("receivedQuery", receiveQuery);
                            String[] receiveKeyValue = SimpleDhtActivity.splitPacket(receiveQuery, "&");
                            rowValues[0] = receiveKeyValue[0];
                            rowValues[1] = receiveKeyValue[1];
                            matrixCursor.addRow(rowValues);
                        }
                    }
                    // Add the local tump to the global dump in a matrix cursor
                    Map<String, ?> allKeyValue = sharedPreferences.getAll();
                    if(allKeyValue!=null)
                    {
                        for(Map.Entry<String,?> keyValue : allKeyValue.entrySet())
                        {
                            rowValues[0] = keyValue.getKey();
                            rowValues[1] = keyValue.getValue().toString();
                            matrixCursor.addRow(rowValues);
                            Log.v("PA3allLocalVaues", "key : "+ keyValue.getKey() + " value: "+keyValue.getValue().toString());
                        }
                    }
                    Log.e("#ofrows", String.valueOf(matrixCursor.getCount()));
                    return matrixCursor;
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    e.printStackTrace();
                }
            }
            // global dump if single avd is alive
            else {
                Map<String, ?> allKeyValue = sharedPreferences.getAll();
                for(Map.Entry<String,?> keyValue : allKeyValue.entrySet())
                {
                    rowValues[0] = keyValue.getKey();
                    rowValues[1] = keyValue.getValue().toString();
                    matrixCursor.addRow(rowValues);
                    Log.v("PA3allLocalVaues", "key : "+ keyValue.getKey() + " value: "+keyValue.getValue().toString());
                }
            }
        }
        else {
            if(senderPort.equals(""))
                senderPort = myPort;
            String value = sharedPreferences.getString(key, null);
            Log.v("PA3testQuery", key + " : " + value);

            // For multiple avd's
            if(status==true && !number_joins.equals(1))
            {
                // queried value found
                if(value!=null) {

                    // if queried value is found on the requesting and return the value
                    if(senderPort.equals(myPort))
                    {
                        rowValues[0] = key;
                        rowValues[1] = value;
                        // Add the values to the matrixCursor Object
                        matrixCursor.addRow(rowValues);
                        return matrixCursor;
                    }
                    else
                    {
                        // if key is found elsewhere
                        Log.e("Keyelsewhere", key+":"+myPort);
                        rowValues[0] = key;
                        rowValues[1] = value;
                        matrixCursor.addRow(rowValues);
                        return matrixCursor;
                    }
                }
                else
                {
                    // queried value not found, redirect
                    Log.e("queryredir", "queryRedirection");
                    Log.e("queryParamreach", successor);
                    //String[] sendQueryRequest= {senderPort+"&"+key, getKey(successor), QUERY_SINGLE};
                    String receiveQueryRequest = "";
                    try {
                        AsyncTask<String, Void, String> querySingle = new SimpleDhtActivity.ClientTask();
                        receiveQueryRequest = querySingle.executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, senderPort+"&"+key, getKey(successor), QUERY_SINGLE).get();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    } catch (ExecutionException e) {
                        e.printStackTrace();
                    }
                    Log.e("receiveQueryRequest", receiveQueryRequest);
                    String[] receiveQueryPacket = SimpleDhtActivity.splitPacket(receiveQueryRequest, "_");
                    rowValues[0] = receiveQueryPacket[0];
                    rowValues[1] = receiveQueryPacket[1];
                    matrixCursor.addRow(rowValues);
                    return matrixCursor;

                }
            }
            // Predominantly for first stage
            else{
                rowValues[0] = key;
                rowValues[1] = value;
                // Add the values to the matrixCursor Object
                matrixCursor.addRow(rowValues);
                Log.v("PA3testMatrixCursor", String.valueOf(rowValues[0]) + " : " + String.valueOf(rowValues[1]));
            }
        }
        return matrixCursor;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    // Get system port
    public String getSystemPort()
    {
        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        return String.valueOf((Integer.parseInt(portStr) * 2));
    }

    // Construct static table for the port and their hash
    public HashMap<String,String> genHashTable(ArrayList<String> remotePort) throws NoSuchAlgorithmException {
        HashMap<String, String> hashTable = new HashMap<String, String>();
        for(int i=0;i<remotePort.size();i++)
        {
            if(remotePort.get(i).equals("11108"))
            {
                hashTable.put(remotePort.get(i),genHash("5554"));
            }
            else if(remotePort.get(i).equals("11112"))
            {
                hashTable.put(remotePort.get(i),genHash("5556"));
            }
            else if(remotePort.get(i).equals("11116"))
            {
                hashTable.put(remotePort.get(i),genHash("5558"));
            }
            else if(remotePort.get(i).equals("11120"))
            {
                hashTable.put(remotePort.get(i),genHash("5560"));
            }
            else if(remotePort.get(i).equals("11124"))
            {
                hashTable.put(remotePort.get(i),genHash("5562"));
            }
        }
        return hashTable;
    }

    // Generate hash of the particular node / message key
    public String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    // getKey of the value from the hashtable
    public String getKey(String value)
    {
        Iterator it = hashTable.entrySet().iterator();
        while (it.hasNext()){
            Map.Entry pair = (Map.Entry)it.next();
            if(value.equals(pair.getValue().toString()))
            {
                return pair.getKey().toString();
            }
        }
        return null;
    }
}