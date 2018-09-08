package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.Context;
import android.content.SharedPreferences;
import android.os.AsyncTask;
import android.util.Log;
import android.util.Pair;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.concurrent.ExecutionException;

public class FailureHandler {

    public String getRecoverableMessage() throws ExecutionException, InterruptedException {
        String returnRecoverableMessage = null;
        // ask the previous two predecessors
        Pair<String, String> predecessors = SimpleDynamoProvider.predecTable.get(SimpleDynamoProvider.hashedPort);

        // Result from first predecessor
        String resultPredec1 = new SimpleDynamoActivity.ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,"predecessor1", SimpleDynamoProvider.getKey(predecessors.first), SimpleDynamoProvider.RECOVERY, null, null).get();
        Log.e("returnP1", resultPredec1+"_"+SimpleDynamoProvider.getKey(predecessors.first));
        if(resultPredec1==null)
            resultPredec1 = "NoData:";

        // Result from second predecessor
        String resultPredec2 = new SimpleDynamoActivity.ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,"predecessor2", SimpleDynamoProvider.getKey(predecessors.second),SimpleDynamoProvider.RECOVERY, null, null).get();
        if(resultPredec2==null)
            resultPredec2 = "NoData:";
        Log.e("returnP2", resultPredec2+"_"+SimpleDynamoProvider.getKey(predecessors.second));

        //ask the next two successor
        Pair<String, String> successors = SimpleDynamoProvider.coordTable.get(SimpleDynamoProvider.hashedPort);

        // Result from the first successor
        String resultSuccess1 = new SimpleDynamoActivity.ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "successor1", SimpleDynamoProvider.getKey(successors.first), SimpleDynamoProvider.RECOVERY, null,null).get();
        if(resultSuccess1==null)
            resultSuccess1 = "NoData:";
        Log.e("returnS1", resultSuccess1+"_"+SimpleDynamoProvider.getKey(successors.first));

        // Result from the second successor
        String resultSuccess2 = new SimpleDynamoActivity.ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "successor2", SimpleDynamoProvider.getKey(successors.second), SimpleDynamoProvider.RECOVERY, null,null).get();
        if(resultSuccess2==null)
            resultSuccess2 = "NoData:";
        Log.e("returnS2", resultSuccess2+"_"+SimpleDynamoProvider.getKey(successors.second));

        returnRecoverableMessage = resultPredec1+resultPredec2+resultSuccess1+resultSuccess2;
        Log.e("ReturnValue", returnRecoverableMessage);

        return returnRecoverableMessage;
    }
}
