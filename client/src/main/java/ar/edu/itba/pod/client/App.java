package ar.edu.itba.pod.client;

import ar.edu.itba.pod.client.util.AppUtil;
import org.apache.commons.cli.CommandLine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by marlanti on 11/3/17.
 */


public class App {

    private static Logger logger = LoggerFactory.getLogger(Client.class);
    public static void main(String[] args) throws Exception {
        CommandLine cmd = AppUtil.getCommandLine(args);
        logger.info("COMMAND "+cmd.toString());
    }
}
