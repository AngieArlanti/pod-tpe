package ar.edu.itba.pod.client;

import ar.edu.itba.pod.model.Data;
import com.hazelcast.core.IList;

import java.io.*;
//import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class DataReader {

    final static List<String> regionNorte = Arrays.asList("Jujuy", "Salta", "Santiago del estero", "Formosa", "Chaco", "Catamarca", "Corrientes", "Misiones");
    final static List<String> regionCentro = Arrays.asList("Córdoba", "Santa Fe", "Entre Ríos");
    final static List<String> regionCuyo = Arrays.asList("La Rioja", "San Juan", "San Luis", "Mendoza");
    final static List<String> regionBA = Arrays.asList("Buenos Aires", "Ciudad Autónoma de Buenos Aires");
    final static List<String> regionPatagonia = Arrays.asList("La Pampa", "Neuquén", "Río negro", "Chubut", "Santa Cruz", "Tierra del Fuego");

    public static void readToList(final IList<Data> ilist, String inFile){

    //public static void readToList(final List<Data> ilist, String inFile){
        try {
            ClassLoader classLoader = Client.class.getClassLoader();
            String csvFile = classLoader.getResource("census/"+inFile).getPath();

            BufferedReader br;
            String line = "";
            br = new BufferedReader(new FileReader(csvFile));

            while ((line = br.readLine()) != null) {
                String[] token = line.split(",");
                String region = "";
                if (regionNorte.contains(token[3]))
                    region = "Región del Norte Grande Argentino";
                else if (regionCentro.contains(token[3]))
                    region = "Región Centro";
                else if (regionCuyo.contains(token[3]))
                    region = "Región del Nuevo Cuyo";
                else if (regionBA.contains(token[3]))
                    region = "Región Buenos Aires";
                else if (regionPatagonia.contains(token[3]))
                    region = "Región Patagónica";
                else
                    region = "Región sin definir";

                ilist.add(new Data(Integer.valueOf(token[0]), Integer.valueOf(token[1]), token[2], token[3], region));
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

  /*  public static void main(String[] args){
        List<Data> list = new ArrayList<Data>();
        readToList(list,"/Users/agophurmuz/Downloads/census100.csv");

        for (Data data : list){
            System.out.println(data.toString());
        }
    }*/
}
