package edu.buffalo.cse.cse486586.simpledynamo;

public class Utility {

    public static boolean checkKeyAssignment(String key, String coord) {
        String goTo = calculateAssignment(key);
        if (goTo.equals(coord))
            return true;
        return false;
    }
    public static String calculateAssignment(String key)
    {
        String goTo = null;
        for (String hashelement : SimpleDynamoProvider.hashList) {
            if (key.compareTo(hashelement) < 0) {
                //store in this element
                goTo = hashelement;
                break;
            }
        }
        if (goTo == null) {
            goTo = SimpleDynamoProvider.hashList.get(0);
        }
        return goTo;
    }
}
