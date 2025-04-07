package proj;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Main {

    public static void main(String[] args) {

        // Get argument input from the user
        if (args.length == 0) {
            System.out.println("Please provide the Matric Card as an argument.");
            return;
        }

        String matricNumber = args[0];
        Map<String, String> result = preprocess(args[0]);
        ColumnStore cs = new ColumnStore();

        try {

            cs.loadData(Constant.FILEPATH);
            cs.filterData(result.get(Constant.KEY_TOWN_NAME), result.get(Constant.KEY_START_YEAR_MONTH), result.get(Constant.KEY_END_YEAR_MONTH), Constant.AREA);
            // Get filtered data
            List<Double> resalePrices = cs.getResalePrices();
            List<Double> floorAreas = cs.getFloorAreas();

            if (resalePrices.isEmpty()) {
                System.out.println("No data found for the given criteria.");
                writeResultsToCSV(matricNumber, result, "No result");
                return;
            }

            // Calculate statistics
            Statistics stats = new Statistics(resalePrices, floorAreas);

            // Output results to console
            System.out.println("\nStatistics for " + result.get(Constant.KEY_TOWN_NAME) +
                    " from " + result.get(Constant.KEY_START_YEAR_MONTH) +
                    " to " + result.get(Constant.KEY_END_YEAR_MONTH) +
                    " (Area >= " + Constant.AREA + "m²):");
            System.out.println(stats.getAllStatistics());

            writeResultsToCSV(matricNumber, result, stats);

        } catch (Exception e) {
            System.out.println("Error loading data: " + e.getMessage());
            e.printStackTrace();

        }

    }


    // Method to write results to CSV file
    private static void writeResultsToCSV(String matricNumber, Map<String, String> queryParams, Object statsObj) {
        String fileName = "ScanResult_" + matricNumber + ".csv";

        try (FileWriter writer = new FileWriter(fileName)) {
            // Write header
            writer.write("Year,Month,town,Category,Value\n");

            // Extract year and month
            String year = queryParams.get(Constant.KEY_START_YEAR_MONTH).substring(0, 4);
            String month = queryParams.get(Constant.KEY_START_YEAR_MONTH).substring(5, 7);
            String town = queryParams.get(Constant.KEY_TOWN_NAME);

            // If no results were found
            if (statsObj instanceof String) {
                writeStatLine(writer, year, month, town, "Minimum Price", (String)statsObj);
                writeStatLine(writer, year, month, town, "Average Price", (String)statsObj);
                writeStatLine(writer, year, month, town, "Standard Deviation of Price", (String)statsObj);
                writeStatLine(writer, year, month, town, "Minimum Price per Square Meter", (String)statsObj);
                return;
            }

            // Write all statistics
            Statistics stats = (Statistics)statsObj;
            writeStatLine(writer, year, month, town, "Minimum Price",
                    String.format("%.2f", stats.getMinimumPrice()));
            writeStatLine(writer, year, month, town, "Average Price",
                    String.format("%.2f", stats.getAveragePrice()));
            writeStatLine(writer, year, month, town, "Standard Deviation of Price",
                    String.format("%.2f", stats.getStandardDeviation()));
            writeStatLine(writer, year, month, town, "Minimum Price per Square Meter",
                    String.format("%.2f", stats.getMinimumPricePerSquareMeter()));

            System.out.println("Results written to " + fileName);

        } catch (IOException e) {
            System.out.println("Error writing to CSV file: " + e.getMessage());
        }
    }
    // Helper method to write a single statistic line
    private static void writeStatLine(FileWriter writer, String year, String month,
                                      String town, String category, String value) throws IOException {
        writer.write(year + "," + month + "," + town + "," + category + "," + value + "\n");
    }

    // Creating a method for preprocessing
    public static Map<String, String> preprocess(String matricCard) {
        // Extract out the digits from input
        int yearDigit = Integer.parseInt(matricCard.substring(matricCard.length() - 2, matricCard.length() - 1));
        int monthDigit = Integer.parseInt(matricCard.substring(matricCard.length() - 3, matricCard.length() - 2));
        int townDigit = Integer.parseInt(matricCard.substring(matricCard.length() - 4, matricCard.length() - 3));

        // TODO: Handle Edge case for Month 12
        String yearName = Constant.YEARS[yearDigit];
        String townName = Constant.DIGIT_TO_TOWN[townDigit];

        String startYearMonth = yearName + "-0" +  monthDigit;
        String endYearMonth = yearName + "-0" +  (monthDigit+1);

        Map<String, String> result = new HashMap<>();
        result.put(Constant.KEY_TOWN_NAME, townName);
        result.put(Constant.KEY_START_YEAR_MONTH, startYearMonth);
        result.put(Constant.KEY_END_YEAR_MONTH, endYearMonth);

        return result;
    }


}