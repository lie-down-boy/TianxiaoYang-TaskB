package mapreduce;

import java.io.*;
import java.util.*;
import java.util.Map;

public class Reduce {
    public static void main(String[] args) throws Exception {
        HashMap<String, Integer> map = new HashMap<>();
        File file = new File("data\\splitCount");
        File[] files = file.listFiles();
        for (File f : files) {
            BufferedReader bufferedReader = new BufferedReader(new FileReader(f));
            String line;
            //Merge the values of the individual blocks
            while ((line=bufferedReader.readLine())!=null){
                String passengerId=line.split(":")[0];
                int s=Integer.parseInt(line.split(":")[1]);
                if (map.containsKey(passengerId)){
                    map.put(passengerId,map.get(passengerId)+s);
                }
                else {
                    map.put(passengerId,s);
                }

            }
        }

        //sort by number of flights
        ArrayList<Map.Entry<String, Integer>> list = new ArrayList<>(map.entrySet());
        Collections.sort(list, new Comparator<Map.Entry<String, Integer>>() {
            @Override
            public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
                return o2.getValue()- o1.getValue();
            }
        });
        BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter("data\\count.txt"));

        for (Map.Entry<String, Integer> entry : list) {
            String key=entry.getKey();
            Integer value =entry.getValue();
            bufferedWriter.write(key+":"+value);
            bufferedWriter.newLine();
        }
        bufferedWriter.flush();
        bufferedWriter.close();

    }

}
