package ar.edu.itba.pod.query4;

import ar.edu.itba.pod.model.Data;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

public class HogarCountMapper implements Mapper<String, Data, String, Long> {

    @Override
    public void map(String s, Data input, Context<String, Long> context) {
        /*Map<String, List<String>> regions = new HashMap<>();
        regions.put("Región del Norte Grande Argentino", Arrays.asList("Jujuy", "Salta", "Santiago del estero", "Formosa", "Chaco", "Catamarca", "Corrientes", "Misiones"));
        regions.put("Región Centro", Arrays.asList("Córdoba", "Santa Fe", "Entre Ríos"));
        regions.put("Región del Nuevo Cuyo", Arrays.asList("La Rioja", "San Juan", "San Luis", "Mendoza"));
        regions.put("Región Buenos Aires", Arrays.asList("Buenos Aires", "Ciudad Autónoma de Buenos Aires"));
        regions.put("Región Patagónica", Arrays.asList("La Pampa", "Neuquén", "Río negro", "Chubut", "Santa Cruz", "Tierra del Fuego"));
        String province = input.getProvinceName();
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

        context.emit(input.getRegion(), input.getHomeId());

    }
}
