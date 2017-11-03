package ar.edu.itba.pod;

/**
 * Created by sebastian on 11/3/17.
 */
public enum Regions {
    NORTE("Region del Norte Argentino"),
    CUYO("Region del Cuyo"),
    CENTRO("Region Centro"),
    BAIRES("Region Buenos Aires"),
    PATAGONIA("Region Patagonica"),
    UNDEFINED("Region SIN DEFINIR");

    private final String text;

    /**
     * @param text
     */
    private Regions(final String text) {
        this.text = text;
    }

    /* (non-Javadoc)
     * @see java.lang.Enum#toString()
     */
    @Override
    public String toString() {
        return text;
    }

    public static Regions getRegion(String province) {
        switch (province.toLowerCase()) {
            case "tucumán":
            case "salta":
            case "misiones":
            case "chaco":
            case "corrientes":
            case "santiago del estero":
            case "jujuy":
            case "formosa":
            case "catamarca":
                return Regions.NORTE;
            case "la rioja":
            case "mendoza":
            case "san juan":
            case "san luis":
                return Regions.CUYO;
            case "santa fe":
            case "córdoba":
            case "entre ríos":
                return Regions.CENTRO;
            case "río negro":
            case "neuquén":
            case "chubut":
            case "la pampa":
            case "santa cruz":
            case "tierra del fuego":
                return Regions.PATAGONIA;
            case "ciudad autónoma de buenos aires":
            case "buenos aires":
                return Regions.BAIRES;
            default:
                return Regions.UNDEFINED;
        }

    }
}
