package proj;

import java.util.List;
import java.util.Map;

public class RunOptimized {

    public static void runAndOutput(String matricNumber, Map<String, String> result, String partitionBy) throws Exception {
        Statistics stats = run(result, partitionBy);

        if (stats == null) {
            throw new RuntimeException("No result found after filtering");
        }

        System.out.println("\nStatistics for " + result.get(Constant.KEY_TOWN_NAME) +
                " from " + result.get(Constant.KEY_START_YEAR_MONTH) +
                " to " + result.get(Constant.KEY_END_YEAR_MONTH) +
                " (Area >= " + Constant.AREA + "m^2):");

        System.out.println(stats.getAllStatistics());

        Util.writeResultsToCSV(matricNumber, result, stats);
    }

    private static Statistics run(Map<String, String> result, String partitionBy) throws Exception {
        ColumnStore cs = (partitionBy == null) ? new ColumnStore() : new ColumnStore(partitionBy);
        cs.loadData(Constant.FILEPATH);

        cs.filterDataByDateRange("month", result.get(Constant.KEY_START_YEAR_MONTH), result.get(Constant.KEY_END_YEAR_MONTH));
        cs.filterDataByEquality("town", result.get(Constant.KEY_TOWN_NAME));
        cs.filterDataByRange("floor_area_sqm", ">=", Constant.AREA);

        List<Object> resalePrices = cs.getColumnValues("resale_price");
        List<Object> floorAreas = cs.getColumnValues("floor_area_sqm");

        if (resalePrices.isEmpty() || floorAreas.isEmpty()) {
            return null;
        }

        return new Statistics(Util.castToDouble(resalePrices), Util.castToDouble(floorAreas));
    }
}
