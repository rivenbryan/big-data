package proj;

import java.util.List;

/**
 * Statistics class to calculate various statistics on filtered HDB resale data
 * Implements four required statistical measures:
 * - Minimum price
 * - Average price
 * - Standard deviation of price
 * - Minimum price per square meter
 */
public class Statistics {

    private final List<Double> resalePrices;
    private final List<Double> floorAreas;

    /**
     * Constructor that takes filtered resale prices and floor areas for calculations
     *
     * @param resalePrices List of filtered resale prices
     * @param floorAreas List of corresponding floor areas
     */
    public Statistics(List<Double> resalePrices, List<Double> floorAreas) {
        this.resalePrices = resalePrices;
        this.floorAreas = floorAreas;
    }

    /**
     * Calculates the minimum resale price in the filtered data
     *
     * @return The minimum resale price or Double.NaN if no data
     */
    public double getMinimumPrice() {
        if (resalePrices.isEmpty()) {
            return Double.NaN;
        }

        return resalePrices.stream()
                .mapToDouble(Double::doubleValue)
                .min()
                .orElse(Double.NaN);
    }

    /**
     * Calculates the average resale price in the filtered data
     *
     * @return The average resale price or Double.NaN if no data
     */
    public double getAveragePrice() {
        if (resalePrices.isEmpty()) {
            return Double.NaN;
        }

        return resalePrices.stream()
                .mapToDouble(Double::doubleValue)
                .average()
                .orElse(Double.NaN);
    }

    /**
     * Calculates the standard deviation of resale prices in the filtered data
     * Formula: sqrt(sum((x - mean)²) / n)
     *
     * @return The standard deviation or Double.NaN if no data or only one data point
     */
    public double getStandardDeviation() {
        if (resalePrices.isEmpty() || resalePrices.size() == 1) {
            return Double.NaN;
        }

        double mean = getAveragePrice();
        double temp = 0;

        for (Double price : resalePrices) {
            temp += Math.pow(price - mean, 2);
        }

        return Math.sqrt(temp / resalePrices.size());
    }

    /**
     * Calculates the minimum price per square meter in the filtered data
     *
     * @return The minimum price per square meter or Double.NaN if no data
     */
    public double getMinimumPricePerSquareMeter() {
        if (resalePrices.isEmpty() || floorAreas.isEmpty() || resalePrices.size() != floorAreas.size()) {
            return Double.NaN; // No data or mismatched lists
        }

        double minPricePerSqm = Double.MAX_VALUE;

        for (int i = 0; i < resalePrices.size(); i++) {
            double pricePerSqm = resalePrices.get(i) / floorAreas.get(i);
            if (pricePerSqm < minPricePerSqm) {
                minPricePerSqm = pricePerSqm;
            }
        }

        return minPricePerSqm == Double.MAX_VALUE ? Double.NaN : minPricePerSqm;
    }

    /**
     * Get all statistics formatted for output
     * Results are rounded to 2 decimal places
     *
     * @return A string containing all statistics
     */
    public String getAllStatistics() {
        StringBuilder sb = new StringBuilder();

        sb.append("Minimum Price: ").append(String.format("%.2f", getMinimumPrice())).append("\n");
        sb.append("Average Price: ").append(String.format("%.2f", getAveragePrice())).append("\n");
        sb.append("Standard Deviation of Price: ").append(String.format("%.2f", getStandardDeviation())).append("\n");
        sb.append("Minimum Price per Square Meter: ").append(String.format("%.2f", getMinimumPricePerSquareMeter())).append("\n");

        return sb.toString();
    }

    /**
     * Get a specific statistic by name
     *
     * @param statName The name of the statistic to retrieve ("Minimum Price", "Average Price", etc.)
     * @return The value of the requested statistic, rounded to 2 decimal places
     */
    public double getStatisticByName(String statName) {
        switch (statName) {
            case "Minimum Price":
                return Math.round(getMinimumPrice() * 100) / 100.0;
            case "Average Price":
                return Math.round(getAveragePrice() * 100) / 100.0;
            case "Standard Deviation of Price":
                return Math.round(getStandardDeviation() * 100) / 100.0;
            case "Minimum Price per Square Meter":
                return Math.round(getMinimumPricePerSquareMeter() * 100) / 100.0;
            default:
                return Double.NaN;
        }
    }
    
    public double getValueForCategory(String category) {
        switch (category) {
            case "Minimum Price":
                return getMinimumPrice();
            case "Average Price":
                return getAveragePrice();
            case "Standard Deviation of Price":
                return getStandardDeviation();
            case "Minimum Price per Square Meter":
                return getMinimumPricePerSquareMeter();
            default:
                return Double.NaN;
        }
    }
}