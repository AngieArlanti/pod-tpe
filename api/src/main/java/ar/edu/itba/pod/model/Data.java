package ar.edu.itba.pod.model;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;

public class Data implements DataSerializable{

    private ActivityCondition activityCondition;
    private Long homeId;
    private String departmentName;
    private String provinceName;
    private String region;

    public Data() {
    }

    public Data(int activityConditionId, Long homeId, String departmentName, String provinceName, String region){
        this.activityCondition = ActivityCondition.values()[activityConditionId];
        this.homeId = homeId;
        this.departmentName = departmentName;
        this.provinceName = provinceName;
        this.region = region;
    }

    public String getRegion() {
        return region;
    }

    public Long getHomeId() {
        return homeId;
    }

    public String getDepartmentName() { return departmentName; }

    public String getProvinceName() {
        return provinceName;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(activityCondition.ordinal());
        out.writeLong(homeId);
        out.writeUTF(departmentName);
        out.writeUTF(provinceName);
        out.writeUTF(region);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        activityCondition = ActivityCondition.values()[in.readInt()];
        homeId = in.readLong();
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
