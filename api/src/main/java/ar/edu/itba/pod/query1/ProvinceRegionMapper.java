package ar.edu.itba.pod.query1;

import ar.edu.itba.pod.model.Data;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

import java.util.*;


public class ProvinceRegionMapper implements Mapper<String, Data, String, Integer>{

    private Map<String, List<String>> regions = new HashMap<>();

    @Override
    public void map(String keyIn, Data valueIn, Context<String, Integer> context) {

        /*Map<String, List<String>> regions = new HashMap<>();
        regions.put("Región del Norte Grande Argentino", Arrays.asList("Jujuy", "Salta", "Santiago del estero", "Formosa", "Chaco", "Catamarca", "Corrientes", "Misiones"));
        regions.put("Región Centro", Arrays.asList("Córdoba", "Santa Fe", "Entre Ríos"));
        regions.put("Región del Nuevo Cuyo", Arrays.asList("La Rioja", "San Juan", "San Luis", "Mendoza"));
        regions.put("Región Buenos Aires", Arrays.asList("Buenos Aires", "Ciudad Autónoma de Buenos Aires"));
        regions.put("Región Patagónica", Arrays.asList("La Pampa", "Neuquén", "Río negro", "Chubut", "Santa Cruz", "Tierra del Fuego"));
        String province = valueIn.getProvinceName();
        String region = "";
        if(regions.get("Región del Norte Grande Argentino").contains(province)){
            region = "Región del Norte Grande Argentino";
        } else if (regions.get("Región Centro").contains(province)){
            region = "Región Centro";
        } else if(regions.get("Región del Nuevo Cuyo").contains(province)){
            region = "Región del Nuevo Cuyo";
        } else if(regions.get("Región Buenos Aires").contains(province)){
            region = "Región Buenos Aires";
        } else {
            region = "Región Patagónica";
        }*/



        context.emit(valueIn.getRegion(), 1);
    }
}
