package proj;

import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
     * Prioritizes the partition column (if specified), and applies remaining filters in order of increasing distinct values.
     *
     * @param cs          The ColumnStore to filter
     * @param result      The preprocessed filter values
     * @param partitionBy The column used for partitioning, if applicable
     * @throws Exception If filtering fails
     */
    private static void applySmartFilter(ColumnStore cs, Map<String, String> result, String partitionBy) throws Exception {

        Map<String, String> filters = new HashMap<>();
        filters.put("town", result.get(Constant.KEY_TOWN_NAME));
        filters.put("month", result.get(Constant.KEY_START_YEAR_MONTH) + ":" + result.get(Constant.KEY_END_YEAR_MONTH));
        filters.put("floor_area_sqm", String.valueOf(Constant.AREA));

        Set<String> filtered = new HashSet<>();

        if (partitionBy != null && filters.containsKey(partitionBy)) {
            switch (partitionBy) {
                case "month" -> {
                    String[] range = filters.get("month").split(":");
                    cs.filterDataByDateRange("month", range[0], range[1]);
                }
                case "floor_area_sqm" -> {
                    cs.filterDataByRange("floor_area_sqm", ">=", Integer.parseInt(filters.get("floor_area_sqm")));
                }
                default -> {
                    cs.filterDataByEquality(partitionBy, filters.get(partitionBy));
                }
            }
            filtered.add(partitionBy);
        }

        filters.entrySet().stream()
            .filter(e -> !filtered.contains(e.getKey()))
            .sorted(Comparator.comparingInt(e -> Constant.DISTINCT_COUNT.getOrDefault(e.getKey(), Integer.MAX_VALUE)))
            .forEachOrdered(e -> {
                try {
                    String key = e.getKey();
                    String value = e.getValue();
                    switch (key) {
                        case "month" -> {
                            String[] range = value.split(":");
                            cs.filterDataByDateRange("month", range[0], range[1]);
                        }
                        case "floor_area_sqm" -> {
                            cs.filterDataByRange("floor_area_sqm", ">=", Integer.parseInt(value));
                        }
                        default -> {
                            cs.filterDataByEquality(key, value);
                        }
                    }
                } catch (Exception ex) {
                    ex.printStackTrace(); 
                }
            });
    }
}
