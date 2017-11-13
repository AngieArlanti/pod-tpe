package ar.edu.itba.pod.client;

import ar.edu.itba.pod.model.Data;
import com.hazelcast.core.IList;

import java.io.*;
//import java.util.ArrayList;
import java.net.URL;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
        Set<Data> dataSet = null;
        int linesRead = 0;
        Set<Thread> threadSet = new HashSet<>();
        try {
            InputStream is = DataReader.class.getClassLoader().getResourceAsStream(inFile);
            if (is == null) {
                csvFile = DEFAULT_FILE;
                Client.getOutputLogger().warn("File not found: " + inFile + ". Loading default file: " + csvFile);
                is = DataReader.class.getClassLoader().getResourceAsStream(DEFAULT_FILE);
            }
            final Reader aReader = new InputStreamReader(is);
            BufferedReader br = new BufferedReader(aReader);

            Client.getTimeLogger().info("Reading from file: " + csvFile);

            String line = "";

            while ((line = br.readLine()) != null) {
                if(linesRead == 0){
                    dataSet = new HashSet<>();
                }
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

                    //ilist.add(new Data(Integer.valueOf(token[0]), Long.valueOf(token[1]), token[2], token[3], region));
                    if(linesRead < 3000){
                        dataSet.add(new Data(Integer.valueOf(token[0]), Long.valueOf(token[1]), token[2], token[3], region));
                        linesRead ++;
                    }
                    else {
                        Thread t = new Thread(new Lodader(ilist, dataSet));
                        t.run();
                        threadSet.add(t);
                        linesRead = 0;
                    }
                }
            }
            //ilist.addAll(set);
            if (linesRead != 0){
                Thread t = new Thread(new Lodader(ilist, dataSet));
                t.start();
                threadSet.add(t);
            }

            for (Thread t: threadSet) {
                if(t.isAlive()){
                    t.join();
                }
            }

            Client.getTimeLogger().info("File read");
        } catch (FileNotFoundException e) {
            Client.getTimeLogger().error("File not found: " + inFile);
            e.printStackTrace();
            System.exit(1);
        } catch (IOException e) {
            Client.getTimeLogger().error("This other strange error: ", e);
            e.printStackTrace();
            System.exit(1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            long endTime = System.nanoTime();
            long duration = (endTime - startTime);
            Client.getTimeLogger().info("File Managing Time: " + duration/1000000 + " ms");
        }
    }

    private static class Lodader implements Runnable {

        private IList<Data> ilist;
        private Set<Data> set;

        public Lodader(IList<Data> ilist, Set<Data> set) {
            this.ilist = ilist;
            this.set = set;
        }

        @Override
        public void run() {
            ilist.addAll(set);
        }
    }
}
