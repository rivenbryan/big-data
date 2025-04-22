package proj;

import java.util.List;
import java.util.Map;

/**
 * RunSharedScan class handles the execution of the columnar scan using shared scan mode.
 * It performs filtering and calculates all required statistics in a single scan.
 */
public class RunSharedScan {

    /**
     * Executes the shared scan, computes statistics, and outputs the result to console and CSV.
     *
     * @param matricNumber The matric number used for naming the output file
     * @param result        Preprocessed filter values (e.g., town, date range)
     * @param partitionBy   Column used for partitioning (optional)
     * @param zoneMapBy     Column used for zone map indexing (optional)
     * @throws Exception If there is an error during processing or data loading
     */
    public static void runAndOutput(String matricNumber, Map<String, String> result, String partitionBy, String zoneMapBy) throws Exception {
        Statistics stats = run(result, partitionBy, zoneMapBy);

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

    /**
     * Internal logic to load data, apply filters, and compute statistics in shared scan mode.
     *
     * @param result      Preprocessed filter values
     * @param partitionBy Partition column (if any)
     * @param zoneMapBy   Zone map index column (if any)
     * @return Statistics object with all computed metrics
     * @throws Exception If any step of loading or filtering fails
     */
    private static Statistics run(Map<String, String> result, String partitionBy, String zoneMapBy) throws Exception {
        ColumnStore cs = new ColumnStore(partitionBy, zoneMapBy);
        cs.loadData(Constant.FILEPATH);

        applySmartFilter(cs, result, partitionBy);

        List<Object> resalePrices = cs.getColumnValues("resale_price");
        List<Object> floorAreas = cs.getColumnValues("floor_area_sqm");

        if (resalePrices.isEmpty() || floorAreas.isEmpty()) {
            return null;
        }

        return new Statistics(Util.castToDouble(resalePrices), Util.castToDouble(floorAreas));
    }

    /**
     * Applies smart filtering to a ColumnStore instance based on date range, town, and floor area.
     *
     * @param cs          The ColumnStore to filter
     * @param result      The preprocessed filter values
     * @param partitionBy The column used for partitioning, if applicable
     * @throws Exception If filtering fails
     */
    private static void applySmartFilter(ColumnStore cs, Map<String, String> result, String partitionBy) throws Exception {
        if ("town".equals(partitionBy)) {
            cs.filterDataByEquality("town", result.get(Constant.KEY_TOWN_NAME));
            cs.filterDataByDateRange("month", result.get(Constant.KEY_START_YEAR_MONTH), result.get(Constant.KEY_END_YEAR_MONTH));
        } else {
            cs.filterDataByDateRange("month", result.get(Constant.KEY_START_YEAR_MONTH), result.get(Constant.KEY_END_YEAR_MONTH));
            cs.filterDataByEquality("town", result.get(Constant.KEY_TOWN_NAME));
        }
        cs.filterDataByRange("floor_area_sqm", ">=", Constant.AREA);
    }
}
