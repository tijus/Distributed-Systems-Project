package edu.buffalo.cse.cse486586.groupmessenger2;

import android.app.Activity;
import android.content.ContentValues;
import android.content.Context;
import android.net.Uri;
import android.os.AsyncTask;
import android.os.Bundle;
import android.renderscript.RenderScript;
import android.telephony.TelephonyManager;
import android.text.method.ScrollingMovementMethod;
import android.util.Log;
import android.view.Menu;
import android.view.View;
import android.view.ViewDebug;
import android.widget.Button;
import android.widget.EditText;
import android.widget.TextView;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.security.Key;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;
import java.util.concurrent.*;
import java.net.SocketTimeoutException;

import static android.content.ContentValues.TAG;

/**
 * GroupMessengerActivity is the main Activity for the assignment.
 *
 * @author stevko
 *
 */
public class GroupMessengerActivity extends Activity implements View.OnClickListener {

    /* Defining variables */
    static final String[] remotePort = {"11108", "11112", "11116", "11120", "11124"};
    static final int SERVER_PORT = 10000;
    static Uri mUri = null;
    int seq = 0;
    Double currentPriority = 0d;
    Timer t = new Timer();

    int nodeCrashed=0;
    String myPort = null;
    String messageSent = "messagSent";
    String proposePriority = "proposePriority";
    String agreePriority = "agreePriority";

    // Using concurrent datastructure to avoid race condition
    // Ref: https://docs.oracle.com/javase/7/docs/api/java/util/concurrent/package-summary.html
    ConcurrentHashMap<String, Double> processTable = new ConcurrentHashMap<String, Double>();
    HashMap<String, Integer> countTable = new HashMap<String, Integer>();
    HashMap<String, Integer> deliverable = new HashMap<String, Integer>();
    ConcurrentHashMap<String, ConcurrentHashMap<Integer, Double>> trackTable = new ConcurrentHashMap<String, ConcurrentHashMap<Integer, Double>>();
    ConcurrentHashMap<Integer, Double> br = new ConcurrentHashMap<Integer, Double>();
    ConcurrentHashMap<String, Integer> pulse = new ConcurrentHashMap<String, Integer>();

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_group_messenger);

        /* Hack provided by the professor */
        TelephonyManager tel = (TelephonyManager) this.getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = String.valueOf((Integer.parseInt(portStr) * 2));


        TextView tv = (TextView) findViewById(R.id.textView1);
        tv.setMovementMethod(new ScrollingMovementMethod());


        t.schedule(new TimerTask() {
            @Override
            public void run() {
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "heartbeat", myPort, "heartbeat", "", "");
            }
        }, 0, 5000);

        ServerSocket serverSocket = null;
        try {
            serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);

        } catch (IOException e) {
            e.printStackTrace();
        }


        findViewById(R.id.button1).setOnClickListener(
                new OnPTestClickListener(tv, getContentResolver()));

        Button btn = (Button) findViewById(R.id.button4);
        btn.setOnClickListener(this);
    }

    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }

    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        getMenuInflater().inflate(R.menu.activity_group_messenger, menu);
        return true;
    }

    @Override
    public void onClick(View v) {
        EditText editText = (EditText) findViewById(R.id.editText1);
        String msgToSend = editText.getText().toString();
        editText.setText("");
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgToSend, myPort, messageSent, "", "");
    }

    //Double currentPriority = 1d;

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {

            ServerSocket serverSocket = sockets[0];
            String sender_port="";
            String receiver_port="";
            String flag="";
            String priority="";
            String receiverPortId="";

            Socket socket;
            try {
                serverSocket.setSoTimeout(5000);
                Log.e("PA2BServerStatus", "testing hashmap..");
                Log.e("PA2BServerStatus", "Server Listening..");
                while (true) {
                    //serverSocket.setSoTimeout(15000);
                    socket = serverSocket.accept();
                    DataInputStream dataInputStream = new DataInputStream(socket.getInputStream());
                    String received_packet = dataInputStream.readUTF();
                    String[] packet_parts = splitPacket(received_packet, ":");
                    //Log.e("PacketReceived", packet_parts[]);
                    String received_message = packet_parts[0];
                    Log.e("PA2BPacketReceived", "packet received:"+packet_parts[0]);
                    sender_port = packet_parts[1];
                    receiver_port = packet_parts[2];
                    flag = packet_parts[3];
                    priority = packet_parts[4];
                    receiverPortId = packet_parts[5];
                    int sendersIndex = Arrays.asList(remotePort).indexOf(sender_port);
                    sendersIndex = sendersIndex + 1;

                    if (received_message.equals("exit"))
                        break;


                    //Log.v("status-flag",flag);

                    // Multicasted message received
                    if (flag.equals(messageSent)) {
                        Log.e("PA2BMulticastReceive", "---------------------------------------------------------");
                        currentPriority = currentPriority+1;
                        processTable.put(received_message, currentPriority);

                        Log.e("PA2BMessage received ", "message: " + received_message + "sender: " + sender_port + "receiver: " + receiver_port);
                        Log.e("PA2BcheckPTableSize ", String.valueOf(processTable.size()));
                        Log.e("PA2BcheckCTableSize ", String.valueOf(countTable.size()));
                        Log.e("PA2BMulticastReceiveEnd", "---------------------------------------------------------");

                        // proposing the highest sequence number in the local queue while sending proposal message
                        proposePriority(received_message, sender_port, receiver_port, String.valueOf(currentPriority));
                        Log.e("PA2BproposedSEq", String.valueOf(currentPriority));
                    } else if (flag.equals(proposePriority)) {
                        Log.e("PA2BProposaltReceive", received_message+"---------------------------------------------------------");
                       /*
                        * If sequence number exists then conflict arises
                        * Hence, we need to resolve the conflict by having sequence number appended by the senders
                        * process id else make the proposed sequence number as new sequence number of the message
                        * wait till the sequence number of all the process is updated i.e. count is 5
                        * If the count is 5 then take the maximum of all the processes and mulitcast the
                        * agreed sequence number to every other processes in the network
                        * after every message receives the sequence number in the network, mark the message as deliverable
                        * and call progressPublish function here.
                        */


                        Log.e("PA2BInitialPrioritySeq", "priority: " + priority + "message: " + received_message + "sender: " + receiver_port + "receiver: " + sender_port);


                        Log.e("PA2BprocessTableSizeseq", String.valueOf(processTable.size()));


                        // maintaining a track table to evaluate the maximum among the proposed sequence
                        Double proposedPriority = Double.parseDouble(priority);
                        processTable.put(received_message,proposedPriority);
                        br.put(sendersIndex, proposedPriority);
                        trackTable.put(received_message,br);
                        Double maxPriority = calculateMax(br);
                        Log.e("PA2BdetectingFailure", String.valueOf(trackTable.size()));

                        // Appending the sender with the sequence at the time of conflict
                        if(!processTable.containsValue(maxPriority))
                        {
                            processTable.put(received_message,maxPriority);
                        }
                        else
                        {
                            maxPriority = Double.parseDouble(String.valueOf(maxPriority.intValue()) + "." + sendersIndex);
                            processTable.put(received_message,maxPriority);
                        }

                        Double storedPriority = maxPriority;

                        Log.e("PA2BSP", "After: "+storedPriority+":"+proposedPriority+" message: "+received_message);

                        currentPriority = storedPriority;

                        // incrementing count ones the proposal has been successfully received
                        if (!countTable.containsKey(received_message))
                        {
                            countTable.put(received_message, 1);
                        }
                        else
                        {
                            int count = countTable.get(received_message) + 1;
                            countTable.put(received_message, count);
                        }

                        Log.e("PA2BprocessTableSizepri", String.valueOf(processTable.size()));
                        Log.e("PA2BcountSize", "countsize" + String.valueOf(countTable.get(received_message)) + "for message " + received_message);


                        // sending the agreement message ones the proposal from every processor has been received
                        if (countTable.get(received_message) == 5) {
                            Log.e("PA2Bagreement", "countsize" + String.valueOf(countTable.get(received_message)) + "for message " + received_message);
                            agreedPriority(received_message, receiver_port, String.valueOf(storedPriority));
                        }
                        Log.e("PA2BProposalEnd", "---------------------------------------------------------");

                    } else if (flag.equals(agreePriority)) {
                        Log.e("PA2BAgreementReceive", "---------------------------------------------------------");
                        Log.e("PA2BagreedPriority", Double.parseDouble(priority)+": message: "+received_message);

                        // Marking the message as deliverable
                        processTable.put(received_message, Double.parseDouble(priority));
                        deliverable.put(received_message, 1);
                        Log.e("markedDeliverable", String.valueOf(deliverable.size())+ " message: "+received_message);

                        Log.e("PA2BcheckIfQueueEmptyB", String.valueOf(processTable.size()));
                        Log.e("testtablecontent", String.valueOf(processTable.get(received_message)));

                        // commiting message if the message is deliverable and at the peek of the queue
                        while(processTable.size() > 0)
                        {
                            String smallseq = findSmallestSeq(processTable);

                            if(deliverable.containsKey(smallseq))
                            {
                                Log.e("PA2Bpublishprogress", "message: "+smallseq+"priority: "+processTable.get(smallseq));
                                processTable.remove(smallseq);
                                deliverable.remove(smallseq);
                                String []packetToCommit = splitPacket(smallseq,"_");
                                String msgToCommit = packetToCommit[0];
                                publishProgress(msgToCommit);
                            }
                            else
                                break;
                        }

                        Log.e("PA2BcheckIfQueueEmpty", String.valueOf(processTable.size()));

                        Log.e("PA2BAgreementEnd", "---------------------------------------------------------");
                    }
                    else if(flag.equals("heartbeat"))
                    {

                        Log.e("testingHeartbeat", received_message);
                        if(!pulse.containsKey(sender_port))
                        {
                            pulse.put(sender_port,1);
                        }
                        else
                        {
                            int x = pulse.get(sender_port);
                            x = x+1;
                            pulse.put(sender_port,x);
                        }
                        Iterator it = pulse.entrySet().iterator();
                        while (it.hasNext()) {
                            Map.Entry pair = (Map.Entry)it.next();
                            Log.e("testingHeartbeat", "sender: "+ pair.getKey() +"pulse count: "+ pair.getValue());
                        }
                        int to = socketTimeOut(pulse);
                        String key = getKey(pulse,to);

                        int[] pulseDiff = accessRunningNode(pulse, key);
                        Log.e("pulsesize", String.valueOf(pulseDiff.length));

                        Log.e("testingHeartbeat","stopped socket pulse"+String.valueOf(to));
                        Log.e("testingHeartbeat","running socket pulse"+String.valueOf(pulseDiff[0] - to));
                        if(((pulseDiff[0] - to)>=5) && ((pulseDiff[1] - to)>=5) && ((pulseDiff[2] - to)>=5) && ((pulseDiff[3] - to)>=5))
                        {
                            Log.e("testingHeartbeat", "socketTimeout");
                            Log.e("testingHeartbeat", key);

                            int stoppedProcessKey = Arrays.asList(remotePort).indexOf(key)+1;

                            // Failure handling
                            Log.e("testingHeartbeat","processTableBefore size: "+processTable.size());

                            Iterator iter = processTable.entrySet().iterator();
                            while (iter.hasNext()) {
                                Map.Entry pair = (Map.Entry)iter.next();
                                Double seqNo = (Double)pair.getValue();
                                String message = (String)pair.getKey();
                                //Log.e("testingHeartbeat", "message before removal"+message+"sequence no."+String.valueOf(seqNo));
                                // truncate packets from stopped host
                                String []packet = splitPacket(message, "_");
                                if(packet[1].equals(String.valueOf(stoppedProcessKey)))
                                {

                                    processTable.remove(message);
                                }

                            }
                            Log.e("testingHeartbeat","processTableAfter size: "+processTable.size());

                            // Broadcasting messages to everyone except failure node

                            int myPorId = Arrays.asList(remotePort).indexOf(myPort)+1;
                            Iterator iter1 = processTable.entrySet().iterator();
                            while (iter1.hasNext()) {
                                Map.Entry pair = (Map.Entry)iter1.next();
                                Double seqNo = (Double)pair.getValue();
                                String message = (String)pair.getKey();

                                Log.e("testingHeartbeat", "message before removal"+message);
                                Log.e("testingHeartbeat", "testing deliverable"+deliverable.containsKey(message)+" seq "+seqNo);
                                Log.e("testingHeartbeat", "testing deliverable"+countTable.get(message));
                                agreedPriority(message, myPort, String.valueOf(seqNo));


                                // truncate packets from stopped host
                            }
                            TimeUnit.SECONDS.sleep(1);
                            t.cancel();

                            // checking if any message can be commited after queue modification
                            /*while (iter.hasNext()) {
                                Map.Entry pair = (Map.Entry)iter.next();
                                Double seqNo = (Double)pair.getValue();
                                String message = (String)pair.getKey();
                                Log.e("testingHeartbeat", "message after removal"+message);
                                // truncate packets from stopped host
                            }*/



                        }
                        //Log.e("testingHeartbeat","Sender: "+ sender_port + "is alive");
                    }
                }
                socket.close();
            }
            catch (SocketTimeoutException e)
            {
                e.printStackTrace();
            }
            catch (IOException e) {

                //new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "heartbeat", myPort, "heartbeat", "", "");
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            return null;
        }

        protected void onProgressUpdate(String... strings) {
            /*
             * The following code displays what is received in doInBackground().
             */
            // String received from the ServerTask background process
            // triming the string to remove inconsistency during transmission
            String strReceived = strings[0].trim();

            /*
            Checking if the string is received successfully.
             */
            if (strReceived.equals(""))
                Log.d("PA2B" + TAG, "String sent not received...");
            else
                Log.d("PA2B" + TAG, "String received successfully..");

            //Log.d("PA2Bcheckcommit", strReceived);
            //Log.d("PA2Bcheckcommit", strReceived);
            // Writing the received message from the remote connection into the TextView
            TextView remoteTextView = (TextView) findViewById(R.id.textView1);
            remoteTextView.append(strReceived + "\t\n");

            /*
             * The following code creates a file in the AVD's internal storage and stores a file.
             *
             * For more information on file I/O on Android, please take a look at
             * http://developer.android.com/training/basics/data-storage/files.html
             */

            // Content provider inclusion comes here

            mUri = buildUri("content", "edu.buffalo.cse.cse486586.groupmessenger2.provider");
            try {
                ContentValues contentValues = new ContentValues();
                contentValues.put("key", Integer.toString(seq));
                contentValues.put("value", strReceived);
                getApplicationContext().getContentResolver().insert(mUri, contentValues);
                seq = seq + 1;
            } catch (Exception e) {
                Log.e("PA2Berror saving data", "Data cannot be saved in the content view");
            }

            return;
        }
    }

    private int[] accessRunningNode(ConcurrentHashMap<String, Integer> pulse, String key )
    {
        int []result = new int[4];
        Iterator it = pulse.entrySet().iterator();
        int i=0;
        while (it.hasNext()) {
            Map.Entry pair = (Map.Entry)it.next();
            if(!((String)pair.getKey()).equals(key))
            {
                result[i] = (Integer) pair.getValue();
                i++;
            }
        }
        return result;
    }
    private int socketTimeOut(ConcurrentHashMap<String, Integer> pulse) {

        int result = 0;
        Integer min = Integer.MAX_VALUE;
        Iterator it = pulse.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry pair = (Map.Entry)it.next();
            if(((Integer)pair.getValue()).compareTo(min)<0)
            {
                min = (Integer) pair.getValue();
                result = (Integer) pair.getValue();
            }
        }
        return result;
    }

    private String getKey(ConcurrentHashMap<String, Integer> pulse, Integer value) {

        String Key = "";
        Iterator it = pulse.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry pair = (Map.Entry)it.next();
            if(((Integer)pair.getValue()).equals(value))
            {
                Key = (String) pair.getKey();
                break;
            }
        }
        return Key;
    }

    // calculate maximum of the received message
    private Double calculateMax(ConcurrentHashMap<Integer, Double> br) {

        Double maxElement = 0d;
        Iterator it = br.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry pair = (Map.Entry)it.next();
            if((Double)pair.getValue()>maxElement)
            {
                maxElement = (Double)pair.getValue();
            }
        }
        return maxElement;
    }

    private class ClientTask extends AsyncTask<String, Void, Void> {

        // Multicasting from sender
        @Override
        protected Void doInBackground(String... msgs) {

            String msgToSend = msgs[0];
            String senderPort = msgs[1];
            String flag = msgs[2];
            String priority = msgs[3];
            String receiverPort = msgs[4];

            String sendFlag = "";
            if(flag.equals(messageSent))
                sendFlag = messageSent;
            else
                sendFlag = "heartbeat";


            int senderId = Arrays.asList(remotePort).indexOf(myPort)+1;
            msgToSend = msgToSend+"_"+String.valueOf(senderId);
            // https://docs.oracle.com/javase/7/docs/api/java/util/UUID.html
            //UUID uniqueId = UUID.randomUUID();
            //msgToSend = msgToSend + "_"+ String.valueOf(uniqueId);
            Socket socket = null;

            try {
                Log.e("PA2BMessage sending ", msgToSend);
                for (int i = 0; i < 5; i++) {
                    socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(remotePort[i]));

                /*
                 * TODO: Fill in your client code that sends out a message.
                 */
                    /**
                     *  For the ClientTask to send messages, we need to open a dataoutput stream.
                     *  Finally we write the message into the socket using UTF encoding
                     *  source: https://docs.oracle.com/javase/7/docs/api/java/io/DataOutputStream.html
                     *
                     */

                    DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());

                    String receivePort = remotePort[i];

                    String packet = constructPacket(msgToSend, senderPort, receivePort, sendFlag, "initialMessage", String.valueOf(i + 1));
                    dataOutputStream.writeUTF(packet);

                }
            }
            catch (ConnectException e)
            {
                Log.e("testing socket", "socket unreachable");
            }
            catch (UnknownHostException e) {
                Log.e("testing socket", "socket unreachable");

                e.printStackTrace();
            } catch (IOException e) {
                Log.e("testing socket", "socket unreachable");
                e.printStackTrace();
            }

            return null;

        }
    }


    // Sending the proposal message
    public void proposePriority(String msgToSend, String senderPort, String receiverPort, String priority) {
        Log.e("PA2BproposePrioritySt", "message " + msgToSend + "send to " + senderPort + " from " + receiverPort);
        Socket socket = null;

        try {
            socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                    Integer.parseInt(senderPort));

            /*
             * TODO: Fill in your client code that sends out a message.
             */
            /**
             *  For the ClientTask to send messages, we need to open a dataoutput stream.
             *  Finally we write the message into the socket using UTF encoding
             *  source: https://docs.oracle.com/javase/7/docs/api/java/io/DataOutputStream.html
             *
             */

            DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());

            int receiverPortId = Arrays.asList(remotePort).indexOf(receiverPort);
            String packet = constructPacket(msgToSend, receiverPort, senderPort, proposePriority, priority, String.valueOf(receiverPortId + 1));
            //String packet = msgToSend + ":" + receiverPort + ":" + proposePriority + ":" + priority + senderPort;
            //Log.e("PA2Btesting message send", "message " + msgToSend + " send from " + receiverPort);
            dataOutputStream.writeUTF(packet);


        } catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e)
        {
            e.printStackTrace();
        }

    }

    // Sending the agreement message
    public void agreedPriority(String msgToSend, String senderPort, String priority) {
        Log.e("status", "entered priority list");
        Socket socket = null;

        for (int i = 0; i < 5; i++) {
            try {
                socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(remotePort[i]));


                /*
                 * TODO: Fill in your client code that sends out a message.
                 */
                /**
                 *  For the ClientTask to send messages, we need to open a dataoutput stream.
                 *  Finally we write the message into the socket using UTF encoding
                 *  source: https://docs.oracle.com/javase/7/docs/api/java/io/DataOutputStream.html
                 *
                 */

                DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());

                String receiverPort = remotePort[i];

                String packet = constructPacket(msgToSend, senderPort, receiverPort, agreePriority, String.valueOf(priority), String.valueOf(i + 1));
                dataOutputStream.writeUTF(packet);


            } catch (UnknownHostException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }

        }

    }

    // Finding peek of the queue after the message is marked as deliverable
    public String findSmallestSeq(ConcurrentHashMap<String, Double> processTable)
    {
        String smallestSeq = "";
        Double min = Double.MAX_VALUE;
        Iterator it = processTable.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry pair = (Map.Entry)it.next();
            if((Double)pair.getValue()<min)
            {
                min = (Double)pair.getValue();
                smallestSeq = (String)pair.getKey();
            }
        }

        return smallestSeq;
    }


    // spliting packet on receipt
    public String[] splitPacket(String packet, String splitKey) {
        String[] packet_parts = packet.split(splitKey);
        return packet_parts;
    }

    // Construct packet on sent
    public String constructPacket(String message, String sender, String receiver, String flag, String priority, String receiverId)
    {
        return message+":"+sender+":"+receiver+":"+flag+":"+priority+":"+receiverId;
    }


}

