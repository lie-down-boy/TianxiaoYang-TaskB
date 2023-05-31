package mapreduce;


import java.io.*;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


class MapSubTask extends Thread{
    private File file;
    private int index;

    public MapSubTask(File file,int index){
        this.file=file;
        this.index=index;
    }

    @Override
    public void run() {
        try {
            BufferedReader bufferedReader = new BufferedReader(new FileReader(file));
            String line;
            HashMap<String, Integer> map = new HashMap<>();
            while ((line = bufferedReader.readLine())!=null){
                String passengerId=line.split(",")[0];
                if(map.containsKey(passengerId)){
                    map.put(passengerId, map.get(passengerId)+1);
                }
                else {
                    map.put(passengerId,1);
                }
            }
            bufferedReader.close();
            BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter("data\\splitCount\\splitCount" + index));

            //shuffle
            ArrayList<java.util.Map.Entry<String, Integer>> list = new ArrayList<>(map.entrySet());
            Collections.sort(list, new Comparator<java.util.Map.Entry<String, Integer>>() {
                @Override
                public int compare(java.util.Map.Entry<String, Integer> o1, java.util.Map.Entry<String, Integer> o2) {
                    return o1.getKey().compareTo(o2.getKey());
                }
            });

            for (java.util.Map.Entry<String, Integer> entry : list) {
                String key=entry.getKey();
                Integer value =entry.getValue();
                bufferedWriter.write(key+":"+value);
                bufferedWriter.newLine();
            }
            bufferedWriter.flush();
            bufferedWriter.close();

        } catch (Exception e){
            e.printStackTrace();
        }
    }
}




public class Map {

    public static void main(String[] args) {

        ExecutorService executorService = Executors.newCachedThreadPool();

        File file = new File("data\\split");
        File[] files = file.listFiles();
        int index=0;
        for (File f : files) {
            MapSubTask mapSubTask = new MapSubTask(f, index);
            executorService.submit(mapSubTask);
            index++;
        }
        executorService.shutdown();
    }
}
