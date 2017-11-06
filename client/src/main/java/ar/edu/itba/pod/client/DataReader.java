package ar.edu.itba.pod.client;

import ar.edu.itba.pod.model.Data;
import com.hazelcast.core.IList;

import java.io.*;
//import java.util.ArrayList;
import java.net.URL;
import java.util.Arrays;
import java.util.List;

public class DataReader {
    // FIXME I'd do everything on toLowerCase
    final static List<String> regionNorte = Arrays.asList("tucumán", "catamarca", "jujuy", "salta", "santiago del estero", "formosa", "chaco", "catamarca", "corrientes", "misiones");
    final static List<String> regionCentro = Arrays.asList("córdoba", "santa fe", "entre ríos");
    final static List<String> regionCuyo = Arrays.asList("la rioja", "san juan", "san luis", "mendoza");
    final static List<String> regionBA = Arrays.asList("buenos aires", "ciudad autónoma de buenos aires");
    final static List<String> regionPatagonia = Arrays.asList("la pampa", "neuquén", "río negro", "chubut", "santa cruz", "tierra del fuego");
    final static String DEFAULT_FILE = "census/census100.csv";

    public static void readToList(final IList<Data> ilist, String inFile, String provinceRestriction) {

        String csvFile = inFile;
        long startTime = System.nanoTime();
        try {
            InputStream is = DataReader.class.getClassLoader().getResourceAsStream(inFile);
            if (is == null) {
                csvFile = DEFAULT_FILE;
                Client.getLogger().warn("File not found: " + inFile + ". Loading default file: " + csvFile);
                is = DataReader.class.getClassLoader().getResourceAsStream(DEFAULT_FILE);
            }
            final Reader aReader = new InputStreamReader(is);
            BufferedReader br = new BufferedReader(aReader);

            Client.getLogger().info("Reading from file: " + csvFile);

            String line = "";

            while ((line = br.readLine()) != null) {
                String[] token = line.split(",");
                String region = "";
                String provincia = token[3].toLowerCase();
                if((provinceRestriction != null && provinceRestriction.toLowerCase().equals(provincia.toLowerCase())) || provinceRestriction == null) {
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
            }
            Client.getLogger().info("File read");
        } catch (FileNotFoundException e) {
            Client.getLogger().error("File not found: " + inFile);
            e.printStackTrace();
            System.exit(1);
        } catch (IOException e) {
            Client.getLogger().error("This other strange error: ", e);
            e.printStackTrace();
            System.exit(1);
        } finally {
            long endTime = System.nanoTime();
            long duration = (endTime - startTime);
            Client.getLogger().info("File Managing Time: " + duration/1000000 + " ms");
        }
    }

}
