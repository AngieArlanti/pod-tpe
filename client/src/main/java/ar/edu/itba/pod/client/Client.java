package ar.edu.itba.pod.client;

import ar.edu.itba.pod.client.model.InputData;
import ar.edu.itba.pod.client.util.CommandLineUtil;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;

import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.core.HazelcastInstance;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;

import static ar.edu.itba.pod.client.util.QueryUtil.*;

public class Client {

    private static Logger timeLogger ;
    private static Logger outputLogger ;


    public static void main(String[] args) throws ExecutionException, InterruptedException {

        InputData input = CommandLineUtil.getInputData(args);
        System.setProperty("timeFilename", input.getTimeOutPath().getAbsolutePath());
        System.setProperty("outputFilename", input.getOutPathFile().getAbsolutePath());

        timeLogger = LoggerFactory.getLogger("time");
        outputLogger = LoggerFactory.getLogger("output");
        timeLogger.info("pod-map-reduce Client Starting ...");


        int query = input.getQuery();
        String inPath = input.getInPath();
        Integer n = input.getN();
        String province = input.getProvince();
        String clusterName = input.getClusterName();
        String clusterPass = input.getClusterPass();

        timeLogger.info(String.format("Connecting with cluster [%s]", clusterName ));

        ClientConfig clientConfig = new ClientConfig();
        ClientNetworkConfig networkConfig = clientConfig.getNetworkConfig();
        for (String address : input.getAddresses()){
            networkConfig.addAddress(address);
        }
        clientConfig.getGroupConfig().setName(clusterName).setPassword(clusterPass);

        HazelcastInstance hz = HazelcastClient.newHazelcastClient(clientConfig);

        getList(clusterName,hz,inPath,null);
        query1(hz, inPath, clusterName);
        query3(hz, inPath, clusterName);
        query4(hz, inPath, clusterName);
        query5(hz, inPath, clusterName);
        query6(hz, inPath, n, clusterName);
        query7(hz, inPath, n, clusterName);
        query7v2(hz, inPath, n, clusterName);
//        query2(hz, inPath, province, n);
        System.exit(1);

    }

    public static Logger getTimeLogger() {
        return timeLogger;
    }

    public static Logger getOutputLogger() {
        return outputLogger;
    }
}

