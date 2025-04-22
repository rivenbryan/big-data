package proj;

import java.util.Map;

public final class Constant {

    public static final String[] DIGIT_TO_TOWN = {
            "BEDOK",         // 0
            "BUKIT PANJANG", // 1
            "CLEMENTI",      // 2
            "CHOA CHU KANG", // 3
            "HOUGANG",       // 4
            "JURONG WEST",   // 5
            "PASIR RIS",     // 6
            "TAMPINES",      // 7
            "WOODLANDS",     // 8
            "YISHUN"         // 9
    };

    public static final String[] YEARS = {
            "2020",
            "2021",
            "2022",
            "2023",
            "2014",
            "2015",
            "2016",
            "2017",
            "2018",
            "2019"
    };
    
    public static final Map<String, Integer> DISTINCT_COUNT = Map.ofEntries(
    	    Map.entry("month", 121),
    	    Map.entry("town", 26),
    	    Map.entry("flat_type", 7),
    	    Map.entry("block", 2700),
    	    Map.entry("street_name", 567),
    	    Map.entry("storey_range", 17),
    	    Map.entry("floor_area_sqm", 174),
    	    Map.entry("flat_model", 21),
    	    Map.entry("lease_commence_date", 56),
    	    Map.entry("resale_price", 4083)
    	);

    public static final String KEY_TOWN_NAME = "townName";
    public static final String KEY_START_YEAR_MONTH = "startYearMonth";
    public static final String KEY_END_YEAR_MONTH = "endYearMonth";
    public static final String FILEPATH = "ResalePricesSingapore.csv";
    public static final Integer AREA = 80; // Floor area in square meters
    public static final int DEFAULT_BLOCK_SIZE = 8192;
    
    public static final int VARCHAR_BYTES = 24;//We use VARCHAR(22) which has 22+2 bytes of overhead
    public static final int FLOAT_BYTES = 4;
}
