package proj;

import java.util.Map;

public class Main {

    public static void main(String[] args) {
        long startTime = System.currentTimeMillis();

        if (args.length < 1) {
            System.out.println("Please provide the Matric Card as the first argument.");
            return;
        }

        String matricNumber = args[0];
        String mode = (args.length >= 2) ? args[1] : "1";
        String partitionBy = null;

        if (args.length >= 3) {
            String thirdArg = args[2];
            if (thirdArg.startsWith("partitionBy=")) {
                partitionBy = thirdArg.substring("partitionBy=".length());
            }
        }

        System.out.println("Matric Card provided: " + matricNumber);
        System.out.println("Mode selected: " + mode);
        if (partitionBy != null) {
            System.out.println("Partitioning by: " + partitionBy);
        } else {
            System.out.println("No partitioning.");
        }

        Map<String, String> result = Util.preprocess(matricNumber);
        System.out.println("Preprocessing completed.");
        System.out.println("Town: " + result.get(Constant.KEY_TOWN_NAME));
        System.out.println("Start Year-Month: " + result.get(Constant.KEY_START_YEAR_MONTH));
        System.out.println("End Year-Month: " + result.get(Constant.KEY_END_YEAR_MONTH));

        try {
            if (mode.equals("0")) {
                RunUnoptimized.runAndOutput(matricNumber, result, partitionBy);
            } else if (mode.equals("1")) {
                RunOptimized.runAndOutput(matricNumber, result, partitionBy);
            } else {
                throw new IllegalArgumentException("Invalid mode. Must be 0 (unoptimized) or 1 (optimized).");
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
