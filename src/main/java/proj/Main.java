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
        String mode = (args.length >= 2) ? args[1] : "optimized";

        System.out.println("Matric Card provided: " + matricNumber);
        System.out.println("Mode selected: " + mode);

        Map<String, String> result = Util.preprocess(matricNumber);
        System.out.println("Preprocessing completed.");
        System.out.println("Town: " + result.get(Constant.KEY_TOWN_NAME));
        System.out.println("Start Year-Month: " + result.get(Constant.KEY_START_YEAR_MONTH));
        System.out.println("End Year-Month: " + result.get(Constant.KEY_END_YEAR_MONTH));

        try {
            if (mode.equalsIgnoreCase("unoptimized")) {
                RunUnoptimized.runAndOutput(matricNumber, result);
            } else {
                RunOptimized.runAndOutput(matricNumber, result);
            }
        } catch (Exception e) {
            System.out.println("Error occurred during execution: " + e.getMessage());
            e.printStackTrace();
        }

        long endTime = System.currentTimeMillis();
        long totalTimeMillis = endTime - startTime;
        double totalTimeSeconds = totalTimeMillis / 1000.0;

        System.out.println(String.format("Total Execution Time: %.2f seconds", totalTimeSeconds));
    }
}
