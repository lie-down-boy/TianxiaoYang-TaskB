package mapreduce;

import java.io.*;

public class Split {
    public static void main(String[] args) throws Exception{

        BufferedReader bufferedReader = new BufferedReader(new FileReader("data\\AComp_Passenger_data_no_error.csv"));
        int index=0;
        BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter("data\\split\\split0"));

        String line;
        int row=0;
        while ( (line= bufferedReader.readLine())!=null ){

            row++;
            bufferedWriter.write(line);
            bufferedWriter.newLine();

            if(row==128) {
                bufferedWriter.flush();
                bufferedWriter.close();

                index++;
                row=0;
                bufferedWriter=new BufferedWriter(new FileWriter("data\\split\\split"+index));

            }

        }

        bufferedWriter.flush();
        bufferedWriter.close();


    }
}
