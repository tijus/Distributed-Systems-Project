package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.Context;
import android.content.SharedPreferences;
import android.net.Uri;
import android.os.AsyncTask;
import android.util.Log;
import android.util.Pair;

import java.util.concurrent.ExecutionException;

public class ManageMessage {

    // Split packet
    public static String[] splitPacket(String packet, String splitKey) {
        String[] packet_parts = packet.split(splitKey);
        return packet_parts;
    }

    protected static Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }

    protected static synchronized boolean storeMessage(SharedPreferences sharedPreferences, String key, String value){

        SharedPreferences.Editor editor = sharedPreferences.edit();
        editor.putString(key,value);
        if(editor.commit())
        {
            return true;
        }
        return false;

    }

    protected static synchronized boolean deleteMessage(SharedPreferences sharedPreferences, String key)
    {
        SharedPreferences.Editor editor = sharedPreferences.edit();
        editor.remove(key);
        editor.apply();
        if(editor.commit())
            return true;
        return false;

    }

    protected static synchronized String getKey(SharedPreferences sharedPreferences, String key)
    {
        String value = sharedPreferences.getString(key,null);
        {
            if(value!=null)
                return value;
        }
        return null;
    }
}
