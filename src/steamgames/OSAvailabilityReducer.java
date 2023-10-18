/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package steamgames;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
/**
 *
 * @author Miguel Huamani <miguel.huamani.r@uni.pe>
 */
public class OSAvailabilityReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

    
    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

        String mostSoldGame = "";
        long total =0;
        long availabilityWindow=0;
        long availabilityLinux=0;
        long availabilityMac= 0;
         try {
            while (values.hasNext()) {
            String[] parts = values.next().toString().split(",");
            boolean Windows=Boolean.parseBoolean(parts[0]);
            boolean Linux=Boolean.parseBoolean(parts[2]);
            boolean Mac=Boolean.parseBoolean(parts[1]);
            
            total+=1;
            if (Windows) availabilityWindow+=1;
            if (Linux) availabilityLinux+=1;
            if (Mac) availabilityMac+=1;
 
        }
            float windowPorcent=(float)availabilityWindow/total;
            float linuxPorcent=(float)availabilityLinux/total;
            float macPorcent=(float)availabilityMac/total;
            output.collect(new Text("WINDOWS"), new Text("jusgos disponibles: "+availabilityWindow+"  porcentaje:"+windowPorcent));
            output.collect(new Text("LINUX"), new Text("jusgos disponibles: "+availabilityLinux+"  porcentaje:"+linuxPorcent));
            output.collect(new Text("WINDOWS"), new Text("jusgos disponibles: "+availabilityMac+"  porcentaje:"+macPorcent));
            
        } catch (NumberFormatException e) {
            System.err.println("Error en la conversión de " + " " + " a double.");
        }
    }
}
