package ar.edu.itba.pod.model;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;
import java.util.Set;
import java.util.TreeSet;

public class ProvincePairDepartments implements DataSerializable {
    private Set<String> provincePairSet;

    public ProvincePairDepartments() {
        provincePairSet = new TreeSet<>();
    }

    public ProvincePairDepartments(Set<String> provincePairSet, String departmentName) {
        this.provincePairSet = provincePairSet;
    }

    @Override
    public void writeData(ObjectDataOutput objectDataOutput) throws IOException {
        objectDataOutput.writeObject(provincePairSet);
    }

    @Override
    public void readData(ObjectDataInput objectDataInput) throws IOException {
        this.provincePairSet = objectDataInput.readObject();
    }


    public void addPair(String pair) {
        this.provincePairSet.add(pair);
    }

    public Set<String> getProvincePairSet() {
        return provincePairSet;
    }




}
