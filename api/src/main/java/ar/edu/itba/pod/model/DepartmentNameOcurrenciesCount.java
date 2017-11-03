package ar.edu.itba.pod.model;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;

public class DepartmentNameOcurrenciesCount implements DataSerializable, Comparable<DepartmentNameOcurrenciesCount> {

    private String departmentName;
    private Integer count;

    public DepartmentNameOcurrenciesCount() {

    }

    public DepartmentNameOcurrenciesCount(String departmentName, Integer count) {
        this.departmentName = departmentName;
        this.count = count;
    }

    public String getDepartmentName() {
        return departmentName;
    }

    public Integer getCount() {
        return count;
    }

    @Override
    public void writeData(ObjectDataOutput objectDataOutput) throws IOException {
        objectDataOutput.writeUTF(departmentName);
        objectDataOutput.writeInt(count);
    }

    @Override
    public void readData(ObjectDataInput objectDataInput) throws IOException {
        this.departmentName = objectDataInput.readUTF();
        this.count = objectDataInput.readInt();
    }

    @Override
    public String toString() {
        return departmentName + "," + String.valueOf(count);
    }

    @Override
    public int compareTo(DepartmentNameOcurrenciesCount o) {
        return this.count.compareTo(o.getCount());
    }
}
