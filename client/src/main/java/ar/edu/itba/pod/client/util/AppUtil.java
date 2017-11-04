package ar.edu.itba.pod.client.util;

import org.apache.commons.cli.*;
import java.io.File;
import java.nio.file.Paths;

/**
 * Created by marlanti on 11/3/17.
 */

public class AppUtil {

    public static File getDir(CommandLine cmd){
        File dir = null;
        if(!cmd.hasOption("dir")){
            dir = Paths.get(System.getProperty("user.dir")).toFile();
        }else{
            dir = Paths.get(cmd.getOptionValue("dir")).toFile();
        }

        return dir;
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
                .addOption(query)
                .addOption(inPath)
                .addOption(outPath)
                .addOption(timeOutPath)
                .addOption(n)
                .addOption(prov);
    }

    public static CommandLine getCommandLine(String[] args) throws ParseException {

        CommandLine cmd = new DefaultParser().parse(commandLineOptions(), args);
//        String mode = getMode(cmd);
//        validateModeOptions(mode, cmd);
        return cmd;
    }

    private static String getMode(CommandLine cmd) {
        Boolean distribute = cmd.hasOption("d");
        Boolean recover = cmd.hasOption("r");
        if (recover == distribute) {
            System.err.println("Either -d or -r must be specified");
            System.exit(1);
        }
        return distribute ? "d" : "r";
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
