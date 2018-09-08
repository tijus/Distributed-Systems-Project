package edu.buffalo.cse.cse486586.groupmessenger1;

import android.app.Activity;
import android.content.ContentValues;
import android.content.Context;
import android.net.Uri;
import android.os.AsyncTask;
import android.os.Bundle;
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
import java.net.MulticastSocket;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;

/**
 * GroupMessengerActivity is the main Activity for the assignment.
 * 
 * @author stevko
 *
 */

public class GroupMessengerActivity extends Activity implements View.OnClickListener {

    private EditText edit_text;
    private Button button4;
    private Uri mUri;
    int seq = 0;
    static final String[] remote_port = new String[]{"11108","11112","11116","11120","11124"};
    static final int SERVER_PORT = 10000;
    TextView tv;
    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_group_messenger);

        /*
         * TODO: Use the TextView to display your messages. Though there is no grading component
         * on how you display the messages, if you implement it, it'll make your debugging easier.
         */
        tv = (TextView) findViewById(R.id.textView1);
        tv.setMovementMethod(new ScrollingMovementMethod());


        try {
            /*
             * Create a server socket as well as a thread (AsyncTask) that listens on the server
             * port.
             *
             * AsyncTask is a simplified thread construct that Android provides. Please make sure
             * you know how it works by reading
             * http://developer.android.com/reference/android/os/AsyncTask.html
             */
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            Log.e("message from server", "Server listening to the port "+ SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            /*
             * Log is a good way to debug your code. LogCat prints out all the messages that
             * Log class writes.
             *
             * Please read http://developer.android.com/tools/debugging/debugging-projects.html
             * and http://developer.android.com/tools/debugging/debugging-log.html
             * for more information on debugging.
             */
            Log.e("socket created", "Can't create a ServerSocket");
            return;
        }

        /*
         * Registers OnPTestClickListener for "button1" in the layout, which is the "PTest" button.
         * OnPTestClickListener demonstrates how to access a ContentProvider.
         */
        findViewById(R.id.button1).setOnClickListener(
                new OnPTestClickListener(tv, getContentResolver()));
        
        /*
         * TODO: You need to register and implement an OnClickListener for the "Send" button.
         * In your implementation you need to get the message from the input box (EditText)
         * and send it to other AVDs.
         */
        edit_text = (EditText)findViewById(R.id.editText1);
        button4 = (Button)findViewById(R.id.button4);
        button4.setOnClickListener(this);
    }

    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.activity_group_messenger, menu);
        return true;
    }

    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }

    public String getSystemPort()
    {
        TelephonyManager tel = (TelephonyManager) this.getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        return String.valueOf((Integer.parseInt(portStr) * 2));
    }

    @Override
    public void onClick(View v) {

    // Sending message to other AVDs (Multicast)

        String msg = edit_text.getText().toString();
        edit_text.setText("");

        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,msg);
    }

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {

            // Server socket being passed as a parameter to the ServerTask (AsyncTask)
            ServerSocket serverSocket = sockets[0];
            Socket socket;

            /*
             * TODO: Fill in your server code that receives messages and passes them
             * to onProgressUpdate().
             */
            String received_message = "";
            mUri = buildUri("content", "edu.buffalo.cse.cse486586.groupmessenger1.provider");
            try {
                // Server ready to accept host connection / client socket
                while (true) {

                    socket = serverSocket.accept();
                    /**
                     * Byte Array Output Stream: Converting output stream generated from the client into
                     * string Ref: https://developer.android.com/reference/java/io/ByteArrayOutputStream.html
                     */
                    /**
                     * For the ServerTask to receive message an object of the class DataInputStream
                     * needs to instantiated. DataInputStream takes in the InputStream received
                     * from the client socket as a parameter. Since the client has sent the data
                     * encoding it in the UTF format,the Server receives the message using readUTF
                     * function.
                     * source: https://docs.oracle.com/javase/7/docs/api/java/io/DataInputStream.html
                     */
                    DataInputStream dataInputStream = new DataInputStream(socket.getInputStream());
                    received_message = dataInputStream.readUTF();

                    // Server is alive infinitely uptil breaking condition is satisfied
                    if(received_message.equals("exit"))
                        break;
                    /**
                     * publishing progress i.e. onProgressUpdate call back and passing the received
                     * string into it
                     */
                    publishProgress(received_message);
                }
                socket.close();

            } catch (IOException e) {
                e.printStackTrace();
            }
            return null;
        }

        protected void onProgressUpdate(String...strings) {
            /*
             * The following code displays what is received in doInBackground().
             */
            // String received from the ServerTask background process
            // triming the string to remove inconsistency during transmission
            String strReceived = strings[0].trim();

            /*
            Checking if the string is received successfully.
             */
            if(strReceived.equals("") || strReceived.equals(null))
                Log.d("data not received", "String sent not received...");
            else
                Log.d("data received", "String received successfully..");

            // Writing the received message from the remote connection into the TextView
            tv.append(strReceived + "\t\n");
            // Writing the message sent into the TextView


            /*
             * The following code creates a file in the AVD's internal storage and stores a file.
             *
             * For more information on file I/O on Android, please take a look at
             * http://developer.android.com/training/basics/data-storage/files.html
             */
            /**
             * Inserting the values as a local store using content provider
             * Here sequence number is the key of the key-value store
             */
            try {
                ContentValues contentValues = new ContentValues();
                contentValues.put("key", Integer.toString(seq));
                contentValues.put("value",strReceived );
                getApplicationContext().getContentResolver().insert(mUri, contentValues);
                seq = seq + 1;
            }
            catch (Exception e)
            {
                Log.e("error saving data", "Data cannot be saved in the content view");
            }

            return;
        }
    }

    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {


            try {
                Socket socket0 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(remote_port[0]));
                Socket socket1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(remote_port[1]));
                Socket socket2 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(remote_port[2]));
                Socket socket3 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(remote_port[3]));
                Socket socket4 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(remote_port[4]));

                Log.d("check message", msgs[0]);

                String msgToSend = msgs[0];

                /*
                 * TODO: Fill in your client code that sends out a message.
                 */
                /**
                 *  For the ClientTask to send messages, we need to open a dataoutput stream.
                 *  Finally we write the message into the socket using UTF encoding
                 *  source: https://docs.oracle.com/javase/7/docs/api/java/io/DataOutputStream.html
                 *
                 *  The following code multicasts message to all the connected host and to itself
                 */


                DataOutputStream dataOutputStream0 = new DataOutputStream(socket0.getOutputStream());
                dataOutputStream0.writeUTF(msgToSend);
                DataOutputStream dataOutputStream1 = new DataOutputStream(socket1.getOutputStream());
                dataOutputStream1.writeUTF(msgToSend);
                DataOutputStream dataOutputStream2 = new DataOutputStream(socket2.getOutputStream());
                dataOutputStream2.writeUTF(msgToSend);
                DataOutputStream dataOutputStream3 = new DataOutputStream(socket3.getOutputStream());
                dataOutputStream3.writeUTF(msgToSend);
                DataOutputStream dataOutputStream4 = new DataOutputStream(socket4.getOutputStream());
                dataOutputStream4.writeUTF(msgToSend);

                // Checking if the message is properly sent. Used for debugging.
                Log.d("sent successfully", "String sent sucessfully by " + msgs[0] + msgToSend);

                    /*
                    Unlike ServerTask, the ClientTask does not closes its socket to avoid any possible
                    race condition. Also, keeping the client socket open allows the thread to send
                    and receive the message.
                     */

            } catch (UnknownHostException e) {

            } catch (IOException e) {
                Log.e("exception", e.toString());
            }
            return null;
        }
    }
}
