package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.ContentValues;
import android.content.Context;
import android.content.SharedPreferences;
import android.database.Cursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.os.Build;
import android.os.Bundle;
import android.app.Activity;
import android.provider.ContactsContract;
import android.text.method.ScrollingMovementMethod;
import android.util.Log;
import android.util.Pair;
import android.view.Menu;
import android.widget.TextView;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

public class SimpleDynamoActivity extends Activity {

    Uri mUri=ManageMessage.buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");

    public static final int SERVER_PORT = 10000;
    ServerSocket serverSocket = null;
    Socket sSocket = null;
    protected ConcurrentHashMap<String,String> keyValueLocalStore = new ConcurrentHashMap<String, String>();

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        ServerSocket serverSocket = null;
        try {
            serverSocket = new ServerSocket(SERVER_PORT);
        } catch (IOException e) {
            e.printStackTrace();
        }

        new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR,serverSocket);
        setContentView(R.layout.activity_simple_dynamo);

        Log.e("Uri",String.valueOf(mUri));
        TextView tv = (TextView) findViewById(R.id.textView1);
        tv.setMovementMethod(new ScrollingMovementMethod());

        try {
            SharedPreferences applicationState = getSharedPreferences("ApplicationState", Context.MODE_PRIVATE);
            Log.e("RecoverStore", String.valueOf(applicationState.contains("wasAlive")));
            if(!applicationState.contains("wasAlive"))
            {
                SharedPreferences.Editor applicationEditor = applicationState.edit();
                applicationEditor.putBoolean("wasAlive", true);
                applicationEditor.commit();
            }
            else
            {
                String recoveredData = new FailureHandler().getRecoverableMessage();
                if(recoveredData!=null)
                {
                    Log.e("recoveryData",recoveredData);
                    SharedPreferences preferences = getSharedPreferences("StoredKeyValue", Context.MODE_PRIVATE);
                    String[] recoveredDatum = ManageMessage.splitPacket(recoveredData,":");
                    Log.e("recoverySize", String.valueOf(recoveredDatum.length));
                    for(String keyValuePairs: recoveredDatum)
                    {
                        if(!keyValuePairs.equals("NoData"))
                        {

                            String[] keyValuePair = ManageMessage.splitPacket(keyValuePairs, "&");
                            if(!keyValueLocalStore.contains(keyValuePair[0]))
                            {
                                Log.e("keyValuePair", keyValuePair[0]+":"+keyValuePair[1]);
                                SharedPreferences.Editor editor = preferences.edit();
                                editor.putString(keyValuePair[0],keyValuePair[1]);
                                if(editor.commit())
                                {
                                    keyValueLocalStore.put(keyValuePair[0],keyValuePair[1]);
                                    Log.e("PA3RecoveryStatus", "Recovery Success");

                                }
                            }
                        }
                    }
                }
            }
            // Store message
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }

    public class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override

        protected Void doInBackground(ServerSocket... sockets) {

            serverSocket = sockets[0];
            String receivedMessage="";
            try {

                while (true)
                {
                    sSocket = serverSocket.accept();
                    sSocket.setKeepAlive(false);

                    DataInputStream dataInputStream = new DataInputStream(sSocket.getInputStream());
                    receivedMessage = dataInputStream.readUTF();
                    Log.e("receivedMessage", receivedMessage);
                    String [] receivedPacket = ManageMessage.splitPacket(receivedMessage,"_");
                    String senderPort = receivedPacket[0];
                    String flag = receivedPacket[1];
                    String key = receivedPacket[2];
                    String value = receivedPacket[3];
                    String thisPort = SimpleDynamoProvider.genHash(String.valueOf(Integer.parseInt(SimpleDynamoProvider.myPort)/2));
                    if(flag.equals(SimpleDynamoProvider.INSERT))
                    {
                        SharedPreferences preferences = getSharedPreferences("StoredKeyValue", Context.MODE_PRIVATE);
                        DataOutputStream dataOutputStream = new DataOutputStream(sSocket.getOutputStream());
                        SharedPreferences.Editor editor = preferences.edit();
                        editor.putString(key,value);
                        //ContentValues contentValues = new ContentValues();
                        //contentValues.put("key",key);
                        //contentValues.put("value",value);
                        //Uri insert = getContentResolver().insert(mUri,contentValues);
                        if(editor.commit())
                        {
                            //replicate
                            Log.e("Replicationstatus", "Replication successful"+key);
                            keyValueLocalStore.put(key,value);
                            dataOutputStream.writeUTF(String.valueOf(true));
                        }
                        else
                        {
                            //Log.e("Replicationstatus", "Error while replicating");
                            dataOutputStream.writeUTF(String.valueOf(false));
                        }
                        dataOutputStream.flush();
                    }
                    else if(flag.equals(SimpleDynamoProvider.QUERY_SINGLE))
                    {
                        Log.e("QuerySingle", senderPort+":"+key+":"+value);
                        SharedPreferences sharedPreferences = getSharedPreferences("StoredKeyValue", Context.MODE_PRIVATE);
                        String sendKey = key;
                        String foundValue = ManageMessage.getKey(sharedPreferences, sendKey);
                        String sendKeyValue = sendKey+"_"+foundValue;
                        Log.e("sendQuerySingle", sendKeyValue);
                        DataOutputStream outputStream = new DataOutputStream(sSocket.getOutputStream());
                        outputStream.writeUTF(sendKeyValue);
                        outputStream.flush();
                    }
                    else if(flag.equals(SimpleDynamoProvider.QUERY_ALL))
                    {
                        String sendingResult = "";
                        Cursor queryLocalCursor = getContentResolver().query(mUri, null, "@", null, null);
                        Log.e("QueryGlobalSize", String.valueOf(queryLocalCursor.getCount()));
                        if(queryLocalCursor!=null && queryLocalCursor.getCount()>0)
                        {
                            while (queryLocalCursor.moveToNext())
                            {
                                int keyIndex = queryLocalCursor.getColumnIndex("key");
                                int valueIndex = queryLocalCursor.getColumnIndex("value");
                                String keyLocal = queryLocalCursor.getString(keyIndex);
                                String valueLocal = queryLocalCursor.getString(valueIndex);
                                Log.e("FetchLocalForGlobal", keyLocal + ":" + valueLocal);
                                sendingResult = keyLocal+"&"+valueLocal+":"+sendingResult;
                            }
                        }
                        else
                        {
                            sendingResult = null;
                        }
                        Log.e("sendResultForGlobal", sendingResult);
                        DataOutputStream dataOutputStream = new DataOutputStream(sSocket.getOutputStream());
                        dataOutputStream.writeUTF(sendingResult);
                        dataOutputStream.flush();

                        // refactored dynamo activity for pa4
                    }
                    else if(flag.equals(SimpleDynamoProvider.RECOVERY))
                    {
                        Cursor recoveryCursor = null;
                        String sendingResult = "";
                        if(senderPort.equals("predecessor1"))
                        {
                            recoveryCursor = getContentResolver().query(mUri, null, "P1", null, null);
                        }
                        else if(senderPort.equals("predecessor2"))
                        {
                            recoveryCursor = getContentResolver().query(mUri, null, "P2", null, null);

                        }else if(senderPort.equals("successor1"))
                        {
                            recoveryCursor = getContentResolver().query(mUri, null, "S1", null, null);
                        }else if(senderPort.equals("successor2"))
                        {
                            recoveryCursor = getContentResolver().query(mUri, null, "S2", null, null);
                        }
                        Log.e("RecoveryData", String.valueOf(recoveryCursor.getCount()));
                        DataOutputStream dataOutputStream = new DataOutputStream(sSocket.getOutputStream());
                        if(recoveryCursor!=null && recoveryCursor.getCount()>0)
                        {
                            while(recoveryCursor.moveToNext())
                            {
                                int keyIndex = recoveryCursor.getColumnIndex("key");
                                int valueIndex = recoveryCursor.getColumnIndex("value");
                                String keyRecovery = recoveryCursor.getString(keyIndex);
                                String valueRecovery = recoveryCursor.getString(valueIndex);
                                Log.e("RecoveryData",keyRecovery);
                                sendingResult = keyRecovery+"&"+valueRecovery+":"+sendingResult;
                            }

                            dataOutputStream.writeUTF(sendingResult);
                        }
                        else
                        {

                            dataOutputStream.writeUTF("NoData:");
                        }
                        dataOutputStream.flush();

                    }
                    else if(flag.equals(SimpleDynamoProvider.DELETE))
                    {
                        SharedPreferences sharedPreferences = getSharedPreferences("StoredKeyValue", Context.MODE_PRIVATE);
                        SharedPreferences.Editor editor = sharedPreferences.edit();
                        editor.remove(key);
                        editor.apply();
                        if(editor.commit()) {
                            DataOutputStream dataOutputStream = new DataOutputStream(sSocket.getOutputStream());
                            dataOutputStream.writeUTF("success");
                        }
                    }
                    else if(flag.equals(SimpleDynamoProvider.DELETE_ELSE))
                    {
                        SharedPreferences sharedPreferences = getSharedPreferences("StoredKeyValue", Context.MODE_PRIVATE);
                        SharedPreferences.Editor editor = sharedPreferences.edit();
                        editor.remove(key);
                        editor.apply();
                        if(editor.commit())
                        {
                            Pair<String, String> successors = SimpleDynamoProvider.coordTable.get(SimpleDynamoProvider.hashedPort);
                            try {
                                String deleteR1 = new SimpleDynamoActivity.ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, SimpleDynamoProvider.myPort, SimpleDynamoProvider.getKey(successors.first), SimpleDynamoProvider.DELETE, key, null).get();
                                String deleteR2 = new SimpleDynamoActivity.ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, SimpleDynamoProvider.myPort, SimpleDynamoProvider.getKey(successors.second), SimpleDynamoProvider.DELETE, key, null).get();
                                /*Log.e("Delete1", deleteR1);
                                Log.e("Delete2", deleteR2);*/
                                if(deleteR1!=null || deleteR2!=null)
                                {
                                    Log.e("DeleteElse","DeletingReplica");
                                    DataOutputStream dataOutputStream = new DataOutputStream(sSocket.getOutputStream());
                                    dataOutputStream.writeUTF("success");
                                    dataOutputStream.flush();
                                }
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            } catch (ExecutionException e) {
                                e.printStackTrace();
                            }
                        }
                    }
                }
            } catch (IOException e) {
                //Log.e("IOException", "Socket Dead");
                e.printStackTrace();
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            } catch (Exception e)
            {
                e.printStackTrace();
            }
            return null;
        }
    }

    public static class ClientTask extends AsyncTask<String, Void, String> {

        // Client side code
        @Override
        protected String doInBackground(String... msgs) {

            String source = msgs[0];
            String dest = msgs[1];
            String flag = msgs[2];
            String key = msgs[3];
            String value = msgs[4];

            Log.e("ForwardedDest", dest);
            DataOutputStream dataOutputStream = null;
            try {
                if(flag.equals(SimpleDynamoProvider.INSERT))
                {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(dest));

                    String msgToSend  = source+"_"+flag+"_"+key+"_"+value;
                    Log.e("ForwardedMessage", msgToSend);
                    dataOutputStream = new DataOutputStream(socket.getOutputStream());
                    dataOutputStream.writeUTF(msgToSend);
                    DataInputStream dataInputStream = new DataInputStream(socket.getInputStream());
                    return dataInputStream.readUTF();
                }
                else if(flag.equals(SimpleDynamoProvider.QUERY_SINGLE))
                {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10,0,2,2}),
                            Integer.parseInt(dest));
                    String msgToSend = source+"_"+flag+"_"+key+"_"+value;
                    Log.e("QuerySingle", msgToSend);
                    dataOutputStream = new DataOutputStream(socket.getOutputStream());
                    dataOutputStream.writeUTF(msgToSend);
                    DataInputStream dataInputStream = new DataInputStream(socket.getInputStream());
                    return dataInputStream.readUTF();
                }
                else if(flag.equals(SimpleDynamoProvider.QUERY_ALL))
                {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10,0,2,2}),
                            Integer.parseInt(dest));
                    String msgToSend = source+"_"+flag+"_"+key+"_"+value+"_"+dest;
                    Log.e("QueryAll", msgToSend);
                    dataOutputStream = new DataOutputStream(socket.getOutputStream());
                    dataOutputStream.writeUTF(msgToSend);
                    DataInputStream dataInputStream = new DataInputStream(socket.getInputStream());
                    return dataInputStream.readUTF();
                }
                else if(flag.equals(SimpleDynamoProvider.RECOVERY))
                {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10,0,2,2}),
                            Integer.parseInt(dest));
                    String msgToSend = source+"_"+flag+"_"+key+"_"+value;
                    Log.e("Recovery", msgToSend);
                    dataOutputStream = new DataOutputStream(socket.getOutputStream());
                    dataOutputStream.writeUTF(msgToSend);
                    DataInputStream dataInputStream = new DataInputStream(socket.getInputStream());
                    return dataInputStream.readUTF();
                }
                else if(flag.equals(SimpleDynamoProvider.DELETE))
                {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10,0,2,2}),
                            Integer.parseInt(dest));
                    String msgToSend = source+"_"+flag+"_"+key+"_"+value;
                    Log.e("Recovery", msgToSend);
                    dataOutputStream = new DataOutputStream(socket.getOutputStream());
                    dataOutputStream.writeUTF(msgToSend);
                    DataInputStream dataInputStream = new DataInputStream(socket.getInputStream());
                    return dataInputStream.readUTF();
                }
                else if(flag.equals(SimpleDynamoProvider.DELETE_ELSE))
                {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10,0,2,2}),
                            Integer.parseInt(dest));
                    String msgToSend = source+"_"+flag+"_"+key+"_"+value;
                    Log.e("DeleteElse", msgToSend);
                    dataOutputStream = new DataOutputStream(socket.getOutputStream());
                    dataOutputStream.writeUTF(msgToSend);

                    DataInputStream dataInputStream = new DataInputStream(socket.getInputStream());
                    return dataInputStream.readUTF();
                }

            } catch (IOException e) {
                e.printStackTrace();
            }catch (Exception e)
            {
                e.printStackTrace();
            }
            return null;
        }
    }

    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.simple_dynamo, menu);
        return true;
    }

    /*@Override
    protected void onStop() {
        super.onStop();
        Log.e("StatusDestroy", "Destroyed");
        try {
            serverSocket.close();
            sSocket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }*/
    @Override
    protected void onRestart() {
        super.onRestart();
        Log.e("ActivityRestart", "Restarted");
    }
}
