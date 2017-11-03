package ar.edu.itba.pod.client;

import ar.edu.itba.pod.model.Data;
import com.hazelcast.core.IList;

import java.io.*;
//import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class DataReader {
    // FIXME I'd do everything on toLowerCase
    final static List<String> regionNorte = Arrays.asList("tucumán", "catamarca","jujuy", "salta", "santiago del estero", "formosa", "chaco", "catamarca", "corrientes", "misiones");
    final static List<String> regionCentro = Arrays.asList("córdoba", "santa fe", "entre ríos");
    final static List<String> regionCuyo = Arrays.asList("la rioja", "san juan", "san luis", "mendoza");
    final static List<String> regionBA = Arrays.asList("buenos aires", "ciudad autónoma de buenos aires");
    final static List<String> regionPatagonia = Arrays.asList("la pampa", "neuquén", "río negro", "chubut", "santa cruz", "tierra del fuego");

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
                String provincia = token[3].toLowerCase();
                if (regionNorte.contains(provincia))
                    region = "Región del Norte Grande Argentino";
                else if (regionCentro.contains(provincia))
                    region = "Región Centro";
                else if (regionCuyo.contains(provincia))
                    region = "Región del Nuevo Cuyo";
                else if (regionBA.contains(provincia))
                    region = "Región Buenos Aires";
                else if (regionPatagonia.contains(provincia))
                    region = "Región Patagónica";
                else
                    region = "Región sin definir";

                ilist.add(new Data(Integer.valueOf(token[0]), Long.valueOf(token[1]), token[2], token[3], region));
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
