package proj;

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

    public static final String KEY_TOWN_NAME = "townName";
    public static final String KEY_START_YEAR_MONTH = "startYearMonth";
    public static final String KEY_END_YEAR_MONTH = "endYearMonth";
    public static final String FILEPATH = "ResalePricesSingapore.csv";
    public static final Integer AREA = 80; // Floor area in square meters
    public static final int DEFAULT_BLOCK_SIZE = 8192;
    
    public static final int VARCHAR_BYTES = 24;//We use VARCHAR(22) which has 22+2 bytes of overhead
    public static final int FLOAT_BYTES = 4;
}
