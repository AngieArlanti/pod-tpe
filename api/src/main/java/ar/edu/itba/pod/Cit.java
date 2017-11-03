package ar.edu.itba.pod;

import java.io.Serializable;

/**
 * Created by sebastian on 11/3/17.
 */
public class Cit implements Serializable {
    int i;
    private String department, province;
    private Regions region;
    private Long homeId;

    public Cit(int i, Long homeId, String department, String province) {
        this.i = i;
        this.homeId = homeId;
        this.department = department;
        this.province = province;
        this.region = Regions.getRegion(province);
    }

    public int getI() {
        return i;
    }

    public String getDepartment() {
        return department;
    }

    public String getProvince() {
        return province;
    }

    public Regions getRegion() {
        return region;
    }

    public Long getHomeId() {
        return homeId;
    }
}
