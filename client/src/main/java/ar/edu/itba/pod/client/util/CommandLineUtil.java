package ar.edu.itba.pod.client.util;

import ar.edu.itba.pod.client.model.InputData;
import org.apache.commons.cli.*;

import java.io.File;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by marlanti on 11/3/17.
 */

public class CommandLineUtil {

    private static String NAME_ARG = "Dname";
    private static String PASS_ARG = "Dpass";
    private static String ADDRESSES_ARG = "Daddresses";
    private static String QUERY_ARG = "Dquery";
    private static String IN_PATH_ARG = "DinPath";
    private static String OUT_PATH_ARG = "DoutPath";
    private static String TIME_OUT_PATH_ARG = "DtimeOutPath";
    private static String N_ARG = "Dn";
    private static String PROV_ARG = "Dprov";


    public static InputData getInputData(String[] args) {

        CommandLine cmd = getCommandLine(args);

        String inPath = getInPath(cmd);
        String clusterName = getClusterName(cmd);
        String clusterPass = getClusterPass(cmd);
        List<String> addresses = getAddresses(cmd);
        int query = getQuery(cmd);
        File outPathFile = getOutFile(cmd);
        File timeOutPath = getTimeOutFile(cmd);
        String province = getProvince(cmd);
        Integer n = getN(cmd);

        return new InputData(inPath, clusterName, clusterPass, addresses, query, outPathFile, timeOutPath, province, n);
    }

    public static File getOutFile(CommandLine cmd) {
        return getFile(OUT_PATH_ARG, cmd);
    }

    public static File getTimeOutFile(CommandLine cmd) {
        return getFile(TIME_OUT_PATH_ARG, cmd);
    }

    private static File getFile(String opt, CommandLine cmd) {
        File file = null;
        if (cmd.hasOption(opt)) {
            file = Paths.get(cmd.getOptionValue(opt)).toFile();
        }
        return file;
    }

    public static String getInPath(CommandLine cmd) {
        return getString(IN_PATH_ARG, cmd);
    }

    public static String getClusterName(CommandLine cmd) {
        return getString(NAME_ARG, cmd);
    }

    public static String getClusterPass(CommandLine cmd) {
        return getString(PASS_ARG, cmd);
    }

    public static List<String> getAddresses(CommandLine cmd) {
        String addressesArg = getString(ADDRESSES_ARG, cmd);
        List<String> addresses = Arrays.asList(addressesArg.split(";"));
        return addresses;
    }

    private static String getString(String opt, CommandLine cmd) {
        String string = null;
        if (cmd.hasOption(opt)) {
            string = cmd.getOptionValue(opt).toString();
        }
        return string;
    }

    public static int getQuery(CommandLine cmd) {
        String query = getString(QUERY_ARG, cmd);
        return Integer.parseInt(query);
    }

    public static Integer getN(CommandLine cmd) {
        if (!cmd.hasOption(N_ARG)) {
            return null;
        }
        String nArg = getString(N_ARG, cmd);
        return Integer.parseInt(nArg);
    }

    public static String getProvince(CommandLine cmd) {
        return getString(PROV_ARG, cmd);
    }

//    java -Daddresses=xx.xx.xx.xx;yy.yy.yy.yy -Dquery=1 -DinPath=censo.csv
//-DoutPath=output.txt -DtimeOutPath=time.txt [queryParams] client.MyClient
//            donde
//● xx.xx.xx.xx;yy.yy.yy.yy son las direcciones IP de los nodos,
//● censo.csv es el archivo de entrada con los datos a procesar
//● output.txt es el archivo de salida con los resultados de la query
//● time.txt es el archivo de salida con los timestamp de los tiempos de la lectura del
//    archivo de entrada y de los trabajos map/reduce
//● [queryParams]:
//            ● -Dn=XX para la queries 2, 6 y 7
//            ● -Dprov=XX para la query 2
//            ● Vacío para las otras queries

    private static Options commandLineOptions() {
        Options options = new Options();
        Option name = Option.builder("Dname")
                .required()
                .hasArg()
                .desc("The Cluster's name")
                .build();
        Option pass = Option.builder("Dpass")
                .required()
                .hasArg()
                .desc("The Cluster's password")
                .build();
        Option addresses = Option.builder("Daddresses")
                .required()
                .hasArg()
                .desc("The nodes' ip addresses")
                .build();
        Option query = Option.builder("Dquery")
                .required()
                .hasArg()
                .desc("Query's number")
                .build();
        Option inPath = Option.builder("DinPath")
                .required()
                .hasArg()
                .desc("Data to Process .csv file")
                .build();
        Option outPath = Option.builder("DoutPath")
                .required()
                .hasArg()
                .desc("Query results .txt outFile")
                .build();
        Option timeOutPath = Option.builder("DtimeOutPath")
                .hasArg()
                .required()
                .desc("Out .txt file with fileRead and map/reduce works's timestamps")
                .build();
        Option n = Option.builder("Dn")
                .hasArg()
                .desc("For 2, 6 and 7 queries")
                .build();
        Option prov = Option.builder("Dprov")
                .hasArg()
                .desc("For query 2")
                .build();

        return options.addOption(addresses)
                .addOption(name)
                .addOption(pass)
                .addOption(query)
                .addOption(inPath)
                .addOption(outPath)
                .addOption(timeOutPath)
                .addOption(n)
                .addOption(prov);
    }

    public static CommandLine getCommandLine(String[] args) {

        CommandLine cmd = null;
        try {
            cmd = new DefaultParser().parse(commandLineOptions(), args);
        } catch (ParseException e) {
            e.printStackTrace();
            System.exit(1);
        }

//        validateModeOptions(mode, cmd);
        return cmd;
    }


//    private static void validateModeOptions(String mode, CommandLine cmd) {
//
//        if(mode == "r" && cmd.hasOption("n")){
//            System.err.println("-n parameter can't be used in r mode.");
//            System.exit(1);
//        }
//
//        if(mode == "d" && cmd.hasOption("n")){
//
//        }
//
//        if (!cmd.hasOption("k")) {
//            System.err.println("-k parameter is required for distributing.");
//            System.exit(1);
//        }
//
//        if(!cmd.hasOption("secret")){
//            System.err.println("-secret parameter is required.");
//            System.exit(1);
//        }
//    }

}
