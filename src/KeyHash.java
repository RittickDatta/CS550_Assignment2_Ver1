import java.util.ArrayList;

/**
 * Created by rittick on 2/4/17.
 */
public class KeyHash {
    public static int getKeyHash(String key, int numberOfServers){

        int hashCode = key.hashCode();
        hashCode = hashCode % numberOfServers + 1;
        hashCode = (hashCode < 0)? -hashCode : hashCode;
        if( hashCode == 0){ hashCode += 1;}
        return hashCode;
    }

    public static ArrayList<Integer> generateKeys(int start, int finish,  int numberOfServers){
        ArrayList<Integer> keys = new ArrayList<>();
        ArrayList<Integer> listOfHashCodes = new ArrayList<>();

        for(int i = start; i<=finish; i++){
            keys.add(i);
            listOfHashCodes.add(getKeyHash(Integer.toString(i),numberOfServers));
        }

        return keys;//TODO return list, not hashcodes :)
    }

    public static int checkHashCodeList(ArrayList<Integer> list){
        int numberOfUniquieElements = 0;

        ArrayList<Integer> uniqueList = new ArrayList<>();

        for(Integer i: list){
            if( !uniqueList.contains(i)){
                uniqueList.add(i);
            }
        }

        numberOfUniquieElements = uniqueList.size();

        return numberOfUniquieElements;
    }

    public static void main(String[] args) {
        getKeyHash(Integer.toString(150000),8);
        getKeyHash(Integer.toString(250000),8);
        getKeyHash(Integer.toString(350000),8);
        getKeyHash(Integer.toString(450000),8);
        getKeyHash(Integer.toString(550000),8);
        getKeyHash(Integer.toString(650000),8);
        getKeyHash(Integer.toString(750000),8);
        getKeyHash(Integer.toString(850000),8);

        int start[] = {100000, 200000, 300000, 400000, 500000, 600000, 700000, 800000};
        int finish[] ={199999, 299999, 399999, 499999, 599999, 699999, 799999, 899999};

        for (int i=0,j=0; i<start.length; i++,j++) {
            ArrayList<Integer> integers = generateKeys(start[i], finish[j], 8);
            System.out.println("Number of Hashcodes: "+ integers.size());
            int numberOfUniqueHashCode = checkHashCodeList(integers);
            System.out.println("Number of Unique HashCodes: "+numberOfUniqueHashCode);
        }
    }
}
