package proj;

import java.util.List;
import java.util.Map;

public class RunUnoptimized {

    public static void runAndOutput(String matricNumber, Map<String, String> result) throws Exception {
        Statistics[] statsArray = run(result);

        if (statsArray == null) {
            throw new RuntimeException("No result found after filtering");
        }

        String[] categories = {
            "Minimum Price",
            "Average Price",
            "Standard Deviation of Price",
            "Minimum Price per Square Meter"
        };

        System.out.println("\nStatistics for " + result.get(Constant.KEY_TOWN_NAME) +
                " from " + result.get(Constant.KEY_START_YEAR_MONTH) +
                " to " + result.get(Constant.KEY_END_YEAR_MONTH) +
                " (Area >= " + Constant.AREA + "m^2):");

        for (int i = 0; i < statsArray.length; i++) {
            double value = statsArray[i].getValueForCategory(categories[i]);
            System.out.println(categories[i] + ": " + String.format("%.2f", value));
        }

        Util.writeResultsToCSV(matricNumber, result, statsArray, categories);
    }

    private static Statistics[] run(Map<String, String> result) throws Exception {
        ColumnStore csMin = new ColumnStore();
        csMin.loadData(Constant.FILEPATH);
        csMin.filterDataByDateRange("month", result.get(Constant.KEY_START_YEAR_MONTH), result.get(Constant.KEY_END_YEAR_MONTH));
        csMin.filterDataByEquality("town", result.get(Constant.KEY_TOWN_NAME));
        csMin.filterDataByRange("floor_area_sqm", ">=", Constant.AREA);
        List<Object> resalePricesForMin = csMin.getColumnValues("resale_price");

        ColumnStore csAvg = new ColumnStore();
        csAvg.loadData(Constant.FILEPATH);
        csAvg.filterDataByDateRange("month", result.get(Constant.KEY_START_YEAR_MONTH), result.get(Constant.KEY_END_YEAR_MONTH));
        csAvg.filterDataByEquality("town", result.get(Constant.KEY_TOWN_NAME));
        csAvg.filterDataByRange("floor_area_sqm", ">=", Constant.AREA);
        List<Object> resalePricesForAvg = csAvg.getColumnValues("resale_price");

        ColumnStore csStd = new ColumnStore();
        csStd.loadData(Constant.FILEPATH);
        csStd.filterDataByDateRange("month", result.get(Constant.KEY_START_YEAR_MONTH), result.get(Constant.KEY_END_YEAR_MONTH));
        csStd.filterDataByEquality("town", result.get(Constant.KEY_TOWN_NAME));
        csStd.filterDataByRange("floor_area_sqm", ">=", Constant.AREA);
        List<Object> resalePricesForStd = csStd.getColumnValues("resale_price");

        ColumnStore csMinPerSqm = new ColumnStore();
        csMinPerSqm.loadData(Constant.FILEPATH);
        csMinPerSqm.filterDataByDateRange("month", result.get(Constant.KEY_START_YEAR_MONTH), result.get(Constant.KEY_END_YEAR_MONTH));
        csMinPerSqm.filterDataByEquality("town", result.get(Constant.KEY_TOWN_NAME));
        csMinPerSqm.filterDataByRange("floor_area_sqm", ">=", Constant.AREA);
        List<Object> resalePricesForMinPerSqm = csMinPerSqm.getColumnValues("resale_price");
        List<Object> floorAreasForMinPerSqm = csMinPerSqm.getColumnValues("floor_area_sqm");

        if (resalePricesForMin.isEmpty() || resalePricesForAvg.isEmpty() || resalePricesForStd.isEmpty() || resalePricesForMinPerSqm.isEmpty()) {
            return null;
        }

        Statistics statsMin = new Statistics(Util.castToDouble(resalePricesForMin), Util.castToDouble(floorAreasForMinPerSqm));
        Statistics statsAvg = new Statistics(Util.castToDouble(resalePricesForAvg), Util.castToDouble(floorAreasForMinPerSqm));
        Statistics statsStd = new Statistics(Util.castToDouble(resalePricesForStd), Util.castToDouble(floorAreasForMinPerSqm));
        Statistics statsMinPerSqm = new Statistics(Util.castToDouble(resalePricesForMinPerSqm), Util.castToDouble(floorAreasForMinPerSqm));

        return new Statistics[]{statsMin, statsAvg, statsStd, statsMinPerSqm};
    }
}
