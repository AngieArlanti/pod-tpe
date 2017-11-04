package ar.edu.itba.pod.model;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;
import java.util.Set;
import java.util.TreeSet;

public class DepartmentProvincePair implements DataSerializable {
    private Set<String> provincePair;
    private String departmentName;

    public DepartmentProvincePair() {
        provincePair = new TreeSet<>();
    }

    public DepartmentProvincePair(Set<String> provincePair, String departmentName) {
        this.provincePair = provincePair;
        this.departmentName = departmentName;
    }

    @Override
    public void writeData(ObjectDataOutput objectDataOutput) throws IOException {
        objectDataOutput.writeUTF(departmentName);
        objectDataOutput.writeObject(provincePair);
    }

    @Override
    public void readData(ObjectDataInput objectDataInput) throws IOException {
        this.departmentName = objectDataInput.readUTF();
        this.provincePair = objectDataInput.readObject();
    }

    public void setDepartmentName(String departmentName) {
        this.departmentName = departmentName;
    }

    public void addPair(String pair) {
        this.provincePair.add(pair);
    }

    public Set<String> getProvincePair() {
        return provincePair;
    }

    public String getDepartmentName() {
        return departmentName;
    }


}
