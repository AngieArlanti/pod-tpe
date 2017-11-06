package ar.edu.itba.pod.client.model;

import java.io.File;
import java.util.List;

/**
 * Created by marlanti on 11/5/17.
 */
public class InputData {

    private String inPath;
    private String clusterName;
    private String clusterPass;
    private List<String> addresses;
    private int query;
    private File outPathFile;
    private File timeOutPath;
    private String province;
    private Integer n;

    public InputData(String inPath, String clusterName, String clusterPass, List<String> addresses, int query, File outPathFile, File timeOutPath, String province, Integer n) {
        this.inPath = inPath;
        this.clusterName = clusterName;
        this.clusterPass = clusterPass;
        this.addresses = addresses;
        this.query = query;
        this.outPathFile = outPathFile;
        this.timeOutPath = timeOutPath;
        this.province = province;
        this.n = n;
    }

    public String getInPath() {
        return inPath;
    }

    public void setInPath(String inPath) {
        this.inPath = inPath;
    }

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public String getClusterPass() {
        return clusterPass;
    }

    public void setClusterPass(String clusterPass) {
        this.clusterPass = clusterPass;
    }

    public List<String> getAddresses() {
        return addresses;
    }

    public void setAddresses(List<String> addresses) {
        this.addresses = addresses;
    }

    public int getQuery() {
        return query;
    }

    public void setQuery(int query) {
        this.query = query;
    }

    public File getOutPathFile() {
        return outPathFile;
    }

    public void setOutPathFile(File outPathFile) {
        this.outPathFile = outPathFile;
    }

    public File getTimeOutPath() {
        return timeOutPath;
    }

    public void setTimeOutPath(File timeOutPath) {
        this.timeOutPath = timeOutPath;
    }

    public String getProvince() {
        return province;
    }

    public void setProvince(String province) {
        this.province = province;
    }

    public Integer getN() {
        return n;
    }

    public void setN(Integer n) {
        this.n = n;
    }
}
