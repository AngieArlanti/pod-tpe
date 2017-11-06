package ar.edu.itba.pod.client;

import ar.edu.itba.pod.client.model.InputData;
import ar.edu.itba.pod.client.util.CommandLineUtil;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;

import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.core.HazelcastInstance;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.concurrent.ExecutionException;

import static ar.edu.itba.pod.client.util.QueryUtil.*;

public class Client {

    private static Logger logger = LoggerFactory.getLogger(Client.class);
    private static PrintWriter printWriter;

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        InputData input = CommandLineUtil.getInputData(args);
        System.setProperty("logfilename", input.getTimeOutPath().getAbsolutePath());

        logger = LoggerFactory.getLogger(Client.class);
        logger.info("pod-map-reduce Client Starting ...");
            //if(!input.getOutPathFile().exists()){
            //    input.setOutPathFile(new File(input.getOutPathFile().getAbsolutePath()));
            //}
        try {
            printWriter = new PrintWriter(input.getOutPathFile(), "UTF-8");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }

        int query = input.getQuery();
        String inPath = input.getInPath();
        Integer n = input.getN();
        String province = input.getProvince();
        String clusterName = input.getClusterName();
        String clusterPass = input.getClusterPass();

        logger.info(String.format("Connecting with cluster [%s]", clusterName ));

        ClientConfig clientConfig = new ClientConfig();
        ClientNetworkConfig networkConfig = clientConfig.getNetworkConfig();
        for (String address : input.getAddresses()){
            networkConfig.addAddress(address);
        }
        clientConfig.getGroupConfig().setName(clusterName).setPassword(clusterPass);

        HazelcastInstance hz = HazelcastClient.newHazelcastClient(clientConfig);

        switch (query) {
            case 1:
                query1(hz, inPath, clusterName);
                break;
            case 2:
                query2(hz, inPath, province, n);
                break;
            case 3:
                query3(hz, inPath, clusterName);
                break;
            case 4:
                query4(hz, inPath, clusterName);
                break;
            case 5:
                query5(hz, inPath, clusterName);
                break;
            case 6:
                query6(hz, inPath, n, clusterName);
                break;
            case 7:
                query7(hz, inPath, n, clusterName);
                break;
            case 8:
                logger.info("Query 8 is a second implementation of query 7");
                query7v2(hz, inPath, n, clusterName);
                break;
            default:
                logger.error("Wrong query number, try again using from 1 to 8");
                System.exit(1);
                break;
        }

    }

    public static Logger getLogger() {
        return logger;
    }

}

