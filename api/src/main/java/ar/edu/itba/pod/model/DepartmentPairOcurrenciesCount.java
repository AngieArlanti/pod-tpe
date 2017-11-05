package ar.edu.itba.pod.model;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;

public class DepartmentPairOcurrenciesCount implements DataSerializable{
    private String departmentPair;
    private Integer count;

    public DepartmentPairOcurrenciesCount() {

    }

    public DepartmentPairOcurrenciesCount(String departmentPair, Integer count) {
        this.departmentPair = departmentPair;
        this.count = count;
    }

    public String getDepartmentPair() {
        return departmentPair;
    }

    public Integer getCount() {
        return count;
    }

    @Override
    public void writeData(ObjectDataOutput objectDataOutput) throws IOException {
        objectDataOutput.writeUTF(departmentPair);
        objectDataOutput.writeInt(count);
    }

    @Override
    public void readData(ObjectDataInput objectDataInput) throws IOException {
        this.departmentPair = objectDataInput.readUTF();
        this.count = objectDataInput.readInt();
    }

    @Override
    public String toString() {
        return departmentPair + "," + String.valueOf(count);
    }


}
