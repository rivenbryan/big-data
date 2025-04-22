package proj;

import java.util.Map;

/**
 * Main class serves as the entry point for the columnar storage engine application.
 * Accepts command-line arguments to configure execution options including preprocessing,
 * partitioning, zone map indexing, and scan mode selection.
 */
public class Main {

    /**
     * Main method that parses command-line arguments, performs preprocessing, and executes
     * the selected scan mode (shared or normal) with optional partitioning and zone map indexing.
     *
     * @param args Command-line arguments:
     *             - args[0]: Matric number (required)
     *             - sharedScan=true/false (optional)
     *             - partitionBy=<column> (optional)
     *             - zoneMapBy=<column> (optional)
     */
    public static void main(String[] args) {
        long startTime = System.currentTimeMillis();

        if (args.length < 1) {
            System.out.println("Please provide the Matric Card as the first argument.");
            return;
        }

        String matricNumber = args[0];
        boolean sharedScan = true;
        String partitionBy = null;
        String zoneMapBy = null;

        for (int i = 1; i < args.length; i++) {
            String arg = args[i];

            if (arg.startsWith("sharedScan=")) {
                String value = arg.substring("sharedScan=".length());
                if (value.equalsIgnoreCase("true")) {
                    sharedScan = true;
                } else if (value.equalsIgnoreCase("false")) {
                    sharedScan = false;
                } else {
                    throw new IllegalArgumentException("Invalid sharedScan value. Use true or false.");
                }
            } else if (arg.startsWith("partitionBy=")) {
                partitionBy = arg.substring("partitionBy=".length());
            } else if (arg.startsWith("zoneMapBy=")) {
                zoneMapBy = arg.substring("zoneMapBy=".length());
            } else {
                throw new IllegalArgumentException("Unknown argument: " + arg);
            }
        }

        System.out.println("Matric Card provided: " + matricNumber);
        System.out.println("Shared Scan: " + sharedScan);
        if (partitionBy != null) {
            System.out.println("Partitioning by: " + partitionBy);
        } else {
            System.out.println("No partitioning.");
        }

        if (zoneMapBy != null) {
            System.out.println("Zone Map Enabled on Column: " + zoneMapBy);
        } else {
            System.out.println("No Zone Map used.");
        }

        Map<String, String> result = Util.preprocess(matricNumber);
        System.out.println("Preprocessing completed.");
        System.out.println("Town: " + result.get(Constant.KEY_TOWN_NAME));
        System.out.println("Start Year-Month: " + result.get(Constant.KEY_START_YEAR_MONTH));
        System.out.println("End Year-Month: " + result.get(Constant.KEY_END_YEAR_MONTH));

        try {
            if (sharedScan) {
                RunSharedScan.runAndOutput(matricNumber, result, partitionBy, zoneMapBy);
            } else {
                RunNormal.runAndOutput(matricNumber, result, partitionBy, zoneMapBy);
            }
        } catch (Exception e) {
            System.out.println("Error occurred during execution: " + e.getMessage());
            e.printStackTrace();
        }

        long endTime = System.currentTimeMillis();
        double totalTimeSeconds = (endTime - startTime) / 1000.0;
        System.out.println(String.format("Total Execution Time: %.2f seconds", totalTimeSeconds));
    }
}
