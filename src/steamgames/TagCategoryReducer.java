/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package steamgames;
import java.util.List;
import java.util.ArrayList;
import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
/**
 *
 * @author Miguel Huamani <miguel.huamani.r@uni.pe>
 */
public class TagCategoryReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        
        while (values.hasNext()) {
            String[] parts = values.next().toString().split(",");
            
            String metacriticScore =parts[13];
            String Price=parts[6];
            String languages =parts[8];
            String developers=parts[25];
            String genres=parts[28];
            String dispo="";
            if(Boolean.parseBoolean(parts[10]))dispo+=" Windows ";
            if(Boolean.parseBoolean(parts[12]))dispo+=" Linux ";
            if(Boolean.parseBoolean(parts[11]))dispo+=" Mac ";
            
            String outputValue="  precio:$"+Price+"  metacrítica:"+metacriticScore+" lenguages:"+languages+" desarrolador(es)"+developers+" generos:"+genres+" disponible en"+dispo;
            output.collect(key, new Text(outputValue));
        }
    }
}
