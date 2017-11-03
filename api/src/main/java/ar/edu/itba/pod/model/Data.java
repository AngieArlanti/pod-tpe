package ar.edu.itba.pod.model;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;

public class Data implements DataSerializable{

    private ActivityCondition activityCondition;
    private int homeId;
    private String departmentName;
    private String provinceName;
    private String region;

    public Data() {
    }

    public Data(int activityConditionId, int homeId, String departmentName, String provinceName, String region){
        this.activityCondition = ActivityCondition.values()[activityConditionId];
        this.homeId = homeId;
        this.departmentName = departmentName;
        this.provinceName = provinceName;
        this.region = region;
    }

    public String getRegion() {
        return region;
    }

    public void setRegion(String region) {
        this.region = region;
    }

    public int getHomeId() {
        return homeId;
    }

    public void setHomeId(int homeId) {
        this.homeId = homeId;
    }

    public String getDepartmentName() {
        return departmentName;
    }

    public void setDepartmentName(String departmentName) {
        this.departmentName = departmentName;
    }

    public String getProvinceName() {
        return provinceName;
    }

    public void setProvinceName(String provinceName) {
        this.provinceName = provinceName;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(activityCondition.ordinal());
        out.writeInt(homeId);
        out.writeUTF(departmentName);
        out.writeUTF(provinceName);
        out.writeUTF(region);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        activityCondition = ActivityCondition.values()[in.readInt()];
        homeId = in.readInt();
        departmentName = in.readUTF();
        provinceName = in.readUTF();
        region = in.readUTF();
    }

    @Override
    public String toString() {
        return "activityConditionId=" + activityCondition.toString() +
                ", homeId=" + homeId +
                ", departmentName='" + departmentName + '\'' +
                ", provinceName='" + provinceName + '\'' +
                ", region= " + region +'\'';
    }
}
