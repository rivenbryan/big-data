package proj;

import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Util {

    public static Map<String, String> preprocess(String matricCard) {
        int yearDigit = Integer.parseInt(matricCard.substring(matricCard.length() - 2, matricCard.length() - 1));
        int monthDigit = Integer.parseInt(matricCard.substring(matricCard.length() - 3, matricCard.length() - 2));
        int townDigit = Integer.parseInt(matricCard.substring(matricCard.length() - 4, matricCard.length() - 3));

        if (monthDigit == 0) {
            monthDigit = 10;
        }

        String yearName = Constant.YEARS[yearDigit];
        String townName = Constant.DIGIT_TO_TOWN[townDigit];

        String startYearMonth = yearName + "-" + (monthDigit < 10 ? "0" + monthDigit : monthDigit);
        String endYearMonth = yearName + "-" + (monthDigit + 1 < 10 ? "0" + (monthDigit + 1) : (monthDigit + 1));

        Map<String, String> result = new HashMap<>();
        result.put(Constant.KEY_TOWN_NAME, townName);
        result.put(Constant.KEY_START_YEAR_MONTH, startYearMonth);
        result.put(Constant.KEY_END_YEAR_MONTH, endYearMonth);

        return result;
    }

    public static List<Double> castToDouble(List<Object> objects) {
        return objects.stream()
                .map(o -> {
                    if (o instanceof Double) {
                        return (Double) o;
                    } else {
                        return Double.parseDouble(o.toString());
                    }
                })
                .toList();
    }

    public static void writeResultsToCSV(String matricNumber, Map<String, String> queryParams, Statistics[] statsArray, String[] categories) {
        String fileName = "ScanResult_" + matricNumber + ".csv";

        try (FileWriter writer = new FileWriter(fileName)) {
            writer.write("Year,Month,Town,Category,Value\n");

            String year = queryParams.get(Constant.KEY_START_YEAR_MONTH).substring(0, 4);
            String month = queryParams.get(Constant.KEY_START_YEAR_MONTH).substring(5, 7);
            String town = queryParams.get(Constant.KEY_TOWN_NAME);

            for (int i = 0; i < statsArray.length; i++) {
                double value = statsArray[i].getValueForCategory(categories[i]);
                writeStatLine(writer, year, month, town, categories[i], String.format("%.2f", value));
            }

            System.out.println("Results successfully written to " + fileName);

        } catch (IOException e) {
            System.out.println("Error writing to CSV file: " + e.getMessage());
        }
    }

    public static void writeResultsToCSV(String matricNumber, Map<String, String> queryParams, Statistics stats) {
        String fileName = "ScanResult_" + matricNumber + ".csv";

        try (FileWriter writer = new FileWriter(fileName)) {
            writer.write("Year,Month,Town,Category,Value\n");

            String year = queryParams.get(Constant.KEY_START_YEAR_MONTH).substring(0, 4);
            String month = queryParams.get(Constant.KEY_START_YEAR_MONTH).substring(5, 7);
            String town = queryParams.get(Constant.KEY_TOWN_NAME);

            writeStatLine(writer, year, month, town, "Minimum Price", String.format("%.2f", stats.getMinimumPrice()));
            writeStatLine(writer, year, month, town, "Average Price", String.format("%.2f", stats.getAveragePrice()));
            writeStatLine(writer, year, month, town, "Standard Deviation of Price", String.format("%.2f", stats.getStandardDeviation()));
            writeStatLine(writer, year, month, town, "Minimum Price per Square Meter", String.format("%.2f", stats.getMinimumPricePerSquareMeter()));

            System.out.println("Results successfully written to " + fileName);

        } catch (IOException e) {
            System.out.println("Error writing to CSV file: " + e.getMessage());
        }
    }

    private static void writeStatLine(FileWriter writer, String year, String month, String town, String category, String value) throws IOException {
        writer.write(year + "," + month + "," + town + "," + category + "," + value + "\n");
    }
}