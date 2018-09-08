package edu.buffalo.cse.cse486586.simpledynamo;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

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
import android.util.Pair;

public class SimpleDynamoProvider extends ContentProvider {

	public static HashMap<String, Pair<String, String>> coordTable = new HashMap<String, Pair<String, String>>();
    public static HashMap<String, Pair<String, String>> predecTable = new HashMap<String, Pair<String, String>>();
	public static ArrayList<String> remotePort= new ArrayList<String>(Arrays.asList
			("11108", "11112","11116",
					"11120","11124"));
	public static HashMap<String, String> hashTable = new HashMap<String, String>();
	public static String myPort, successor, predecessor = null;
	public static final String INSERT = "insert";
	public static final String QUERY_SINGLE = "querySingle";
	public static final String QUERY_ALL = "queryAll";
	public static final String RECOVERY = "recovery";
	public static final String DELETE = "delete";
	public static final String DELETE_ELSE = "deleteElse";
	public static String hashedPort = null;
	public static String failedNode = null;
	protected static List<String> hashList;
	@Override
	public synchronized int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub
		SharedPreferences sharedPreferences = getContext().getSharedPreferences("StoredKeyValue", Context.MODE_PRIVATE);
		SharedPreferences.Editor editor = sharedPreferences.edit();

        try {
            if(Utility.checkKeyAssignment(genHash(selection),hashedPort))
            {
                Log.e("checkassingment", "key: "+selection+" present");
                    String key = selection;
                    editor.remove(key);
                    editor.apply();
                    if (!editor.commit()) {
                        return 0;
                    } else {
                        Pair<String, String> successors = coordTable.get(hashedPort);

                            String deleteR1 = new SimpleDynamoActivity.ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, myPort, getKey(successors.first), DELETE, key, null).get();
                            String deleteR2 = new SimpleDynamoActivity.ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, myPort, getKey(successors.second), DELETE, key, null).get();
                            //Log.e("Delete1", deleteR1);
                            //Log.e("Delete2", deleteR2);
                            if(deleteR1!=null || deleteR2!=null)
                            {
                                Log.e("DeleteFromReplica", "deleted data from replica");
                                return 1;
                            }

                }
            }
            else
            {
                Log.e("checkassingment", "key: "+selection+" elsewhere");
                String goTo = Utility.calculateAssignment(genHash(selection));
                String deleteElse = new SimpleDynamoActivity.ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, myPort, getKey(goTo), DELETE_ELSE, selection, null).get();
                if(deleteElse!=null)
                {
                    Log.e("DeleteElse", deleteElse);
                    return 1;
                }

            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        return 0;
	}
	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public synchronized Uri insert(Uri uri, ContentValues values) {
		// TODO Auto-generated method stub
		String key  = values.get("key").toString();
		String value = values.get("value").toString();
		String hashedKey = null;
		try {
            hashedKey = genHash(key);
                String goTo = Utility.calculateAssignment(hashedKey);

                Pair<String, String> replicas = coordTable.get(goTo);
                String first_replica = replicas.first;
                String second_replica = replicas.second;

                String status = new SimpleDynamoActivity.ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, myPort, getKey(goTo), INSERT, key, value).get();
                Log.e("InsertStatusgoTo", status+":"+key);
                if (status == null)
                {
                    //status = new SimpleDynamoActivity.ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, myPort, getKey(goTo), INSERT, key, value).get();
                    failedNode = getKey(goTo);
                }

                String repl1 = new SimpleDynamoActivity.ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, myPort, getKey(first_replica), INSERT, key, value).get();
                Log.e("InsertStatusrepl1", repl1+":"+key);
                if (repl1 == null)
                    if (failedNode == null || !failedNode.equals(getKey(first_replica)))
                        failedNode = getKey(first_replica);

                String repl2 = new SimpleDynamoActivity.ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, myPort, getKey(second_replica), INSERT, key, value).get();
                Log.e("InsertStatusrepl2", repl2+":"+key);
                if (repl2 == null)
                    if (failedNode == null || !failedNode.equals(getKey(second_replica)))
                        failedNode = getKey(second_replica);

                if (failedNode != null)
                    Log.e("failedNode", failedNode);
                Log.e("goTo", key + ":" + hashedKey + ":" + goTo + ":" + status);

            }
		catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        return null;
	}

	private String getMinimumNode(List<String> valueSet) {
		return Collections.min(valueSet);
	}

	private String getMaxNode(List<String> valueSet) {
        return Collections.max(valueSet);
    }

	@Override
	public boolean onCreate() {
		// TODO Auto-generated method stub

		//Log.e("TestCreate", "Enter Create");
		myPort = getSystemPort();
		coordTable.put("177ccecaec32c54b82d5aaafc18a2dadb753e3b1",new Pair<String, String>("208f7f72b198dadd244e61801abe1ec3a4857bc9","33d6357cfaaf0f72991b0ecd8c56da066613c089"));
		coordTable.put("208f7f72b198dadd244e61801abe1ec3a4857bc9",new Pair<String, String>("33d6357cfaaf0f72991b0ecd8c56da066613c089","abf0fd8db03e5ecb199a9b82929e9db79b909643"));
		coordTable.put("33d6357cfaaf0f72991b0ecd8c56da066613c089",new Pair<String, String>("abf0fd8db03e5ecb199a9b82929e9db79b909643","c25ddd596aa7c81fa12378fa725f706d54325d12"));
		coordTable.put("abf0fd8db03e5ecb199a9b82929e9db79b909643",new Pair<String, String>("c25ddd596aa7c81fa12378fa725f706d54325d12","177ccecaec32c54b82d5aaafc18a2dadb753e3b1"));
		coordTable.put("c25ddd596aa7c81fa12378fa725f706d54325d12",new Pair<String, String>("177ccecaec32c54b82d5aaafc18a2dadb753e3b1","208f7f72b198dadd244e61801abe1ec3a4857bc9"));

        predecTable.put("177ccecaec32c54b82d5aaafc18a2dadb753e3b1",new Pair<String, String>("c25ddd596aa7c81fa12378fa725f706d54325d12","abf0fd8db03e5ecb199a9b82929e9db79b909643"));
        predecTable.put("208f7f72b198dadd244e61801abe1ec3a4857bc9",new Pair<String, String>("177ccecaec32c54b82d5aaafc18a2dadb753e3b1","c25ddd596aa7c81fa12378fa725f706d54325d12"));
        predecTable.put("33d6357cfaaf0f72991b0ecd8c56da066613c089",new Pair<String, String>("208f7f72b198dadd244e61801abe1ec3a4857bc9","177ccecaec32c54b82d5aaafc18a2dadb753e3b1"));
        predecTable.put("abf0fd8db03e5ecb199a9b82929e9db79b909643",new Pair<String, String>("33d6357cfaaf0f72991b0ecd8c56da066613c089","208f7f72b198dadd244e61801abe1ec3a4857bc9"));
        predecTable.put("c25ddd596aa7c81fa12378fa725f706d54325d12",new Pair<String, String>("abf0fd8db03e5ecb199a9b82929e9db79b909643","33d6357cfaaf0f72991b0ecd8c56da066613c089"));

		try {
			hashedPort = genHash(String.valueOf(Integer.parseInt(myPort)/2));
			hashTable = genHashTable(remotePort);
            hashList = new ArrayList<String>(hashTable.values());
            Collections.sort(hashList);
            predecessor = getPredecessor(hashList, hashedPort);
            successor = getSuccessor(hashList, hashedPort);
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		return false;
	}

	@Override
	public synchronized Cursor query(Uri uri, String[] projection, String selection,
						String[] selectionArgs, String sortOrder) {

		// TODO Auto-generated method stub
		String senderPort = "";
		if(selection.contains("_"))
		{
			String[] selectionParam = ManageMessage.splitPacket(selection,"_");
			senderPort = selectionParam[0];
			selection = selectionParam[1];
		}

		SharedPreferences sharedPreferences = getContext().getSharedPreferences("StoredKeyValue", Context.MODE_PRIVATE);
		String[] cols = new String[]{"key", "value"};
		MatrixCursor matrixCursor = new MatrixCursor(cols);
		Object[] rowValues = new Object[2];
		if(selection.equals("@"))
		{
			Map<String, ?> allKeyValue = sharedPreferences.getAll();
			for(Map.Entry<String,?> keyValue : allKeyValue.entrySet())
			{
				rowValues[0] = keyValue.getKey();
				rowValues[1] = keyValue.getValue().toString();
				matrixCursor.addRow(rowValues);
				Log.v("PA3allLocalVaues", "key : "+ keyValue.getKey() + " value: "+keyValue.getValue().toString());
			}
		}
		else if(selection.equals("*"))
		{
			for(String port: remotePort)
			{
			    if(!port.equals(myPort))
                {
                    try {
                        Log.e("dest",port);
                        String resultFromPort = new SimpleDynamoActivity.ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, myPort, port, QUERY_ALL, null, null).get();
                        //Log.e("Queryall",resultFromPort);
                        if(resultFromPort!=null)
                        {
                            String []keyValuePairs = ManageMessage.splitPacket(resultFromPort, ":");
                            for(String keyValuePair : keyValuePairs)
                            {
                                String []keyValue = ManageMessage.splitPacket(keyValuePair, "&");
                                rowValues[0] = keyValue[0];
                                rowValues[1] = keyValue[1];
                                matrixCursor.addRow(rowValues);
                            }
                        }
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    } catch (ExecutionException e) {
                        e.printStackTrace();
                    }
                }

			}
		}
        else if(selection.equals("P1")||selection.equals("P2"))
        {
            Map<String, ?> allKeyValue = sharedPreferences.getAll();
            if(allKeyValue!=null)
            {
                for(Map.Entry<String,?> keyValue : allKeyValue.entrySet())
                {
                    try {
                        if(Utility.checkKeyAssignment(genHash(keyValue.getKey()),hashedPort))
                        {
                            rowValues[0] = keyValue.getKey();
                            rowValues[1] = keyValue.getValue().toString();
                            matrixCursor.addRow(rowValues);
                            Log.v("PA3P1", "key : "+ keyValue.getKey() + " value: "+keyValue.getValue().toString());
                        }
                    } catch (NoSuchAlgorithmException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
        else if(selection.equals("S1"))
        {
            Map<String, ?> allKeyValue = sharedPreferences.getAll();
            if(allKeyValue!=null)
            {
                for(Map.Entry<String,?> keyValue : allKeyValue.entrySet())
                {
                    try {
                        if(Utility.checkKeyAssignment(genHash(keyValue.getKey()),predecTable.get(hashedPort).first))
                        {
                            rowValues[0] = keyValue.getKey();
                            rowValues[1] = keyValue.getValue().toString();
                            matrixCursor.addRow(rowValues);
                            Log.v("PA3S1", "key : "+ keyValue.getKey() + " value: "+keyValue.getValue().toString());
                        }
                    } catch (NoSuchAlgorithmException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
        else if(selection.equals("S2"))
        {
            Map<String, ?> allKeyValue = sharedPreferences.getAll();
            if(allKeyValue!=null)
            {
                for(Map.Entry<String,?> keyValue : allKeyValue.entrySet())
                {
                    try {
                        if(Utility.checkKeyAssignment(genHash(keyValue.getKey()),predecTable.get(hashedPort).second))
                        {
                            rowValues[0] = keyValue.getKey();
                            rowValues[1] = keyValue.getValue().toString();
                            matrixCursor.addRow(rowValues);
                            Log.v("PA3S2", "key : "+ keyValue.getKey() + " value: "+keyValue.getValue().toString());
                        }
                    } catch (NoSuchAlgorithmException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
		else {
				String key = selection;
				String hashedKey = null;
				try {
					hashedKey = genHash(key);
					List<String> hashList = new ArrayList<String>(hashTable.values());
					Collections.sort(hashList);
					predecessor = getPredecessor(hashList, hashedPort);
					successor = getSuccessor(hashList, hashedPort);
					String goTo = null;
					for(String hashelement: hashList)
					{
						if(hashedKey.compareTo(hashelement)<0)
						{
							//store in this element
							goTo=hashelement;
							break;
						}
					}
					if(goTo==null)
					{
						goTo = hashList.get(0);
					}
					Log.e("QueryTo", key+":"+hashedKey+":"+goTo);

					String keyValuePair = new SimpleDynamoActivity.ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, myPort, getKey(goTo), QUERY_SINGLE, key, null).get();
					if(keyValuePair==null)
                    {
                        keyValuePair = new SimpleDynamoActivity.ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, myPort, getKey(coordTable.get(goTo).first), QUERY_SINGLE, key, null).get();
                        Log.e("fromReplica", keyValuePair);
                        String[] keyValue = ManageMessage.splitPacket(keyValuePair, "_");
                        rowValues[0] = keyValue[0];
                        rowValues[1] = keyValue[1];
                        matrixCursor.addRow(rowValues);
                    }
                    else
                    {
                        Log.e("RecoveryQuery", keyValuePair);
                        String[] keyValue = ManageMessage.splitPacket(keyValuePair, "_");
                        rowValues[0] = keyValue[0];
                        rowValues[1] = keyValue[1];
                        matrixCursor.addRow(rowValues);
                    }

					return matrixCursor;

				} catch (NoSuchAlgorithmException e) {
					e.printStackTrace();
				} catch (InterruptedException e) {
					e.printStackTrace();
				} catch (ExecutionException e) {
					e.printStackTrace();
				} catch (NullPointerException e){

                }
			}


		return matrixCursor;
	}
	@Override
	public int update(Uri uri, ContentValues values, String selection,
					  String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
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

	protected static String genHash(String input) throws NoSuchAlgorithmException {
		MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
		byte[] sha1Hash = sha1.digest(input.getBytes());
		Formatter formatter = new Formatter();
		for (byte b : sha1Hash) {
			formatter.format("%02x", b);
		}
		return formatter.toString();
	}

	// Get system port
	public String getSystemPort()
	{
		TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
		String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		return String.valueOf((Integer.parseInt(portStr) * 2));
	}

	// Construct static table for the port and their hash
	public static  HashMap<String,String> genHashTable(ArrayList<String> remotePort) throws NoSuchAlgorithmException {
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

	// getKey of the value from the hashtable
	public static String getKey(String value)
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
