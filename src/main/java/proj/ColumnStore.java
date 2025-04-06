package proj;

import java.io.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class ColumnStore {

    private final List<String> months;      // Format: YYYY-MM
    private final List<String> towns;       // Town names
    private final List<Double> floorAreas;  // Floor areas in square meters
    private final List<Double> resalePrices;// Resale prices in SGD
    private final Set<Integer> indexSet = new HashSet<>();

    public ColumnStore() {
        this.months = new ArrayList<>();
        this.towns = new ArrayList<>();
        this.floorAreas = new ArrayList<>();
        this.resalePrices = new ArrayList<>();
    }

    public void loadData(String filepath) throws IOException {
        int lineCount = 0;
        InputStream input = getClass().getClassLoader().getResourceAsStream("ResalePricesSingapore.csv");
        try (BufferedReader br = new BufferedReader(new InputStreamReader(input))) {
            String line = br.readLine(); // Skip header row
            System.out.println("Loading data from file: " + filepath);


            while ((line = br.readLine()) != null) {
                String[] values = line.split(",");
                // Store each column value in its respective array
                months.add(values[0].trim());
                towns.add(values[1].trim());
                floorAreas.add(Double.parseDouble(values[6].trim()));
                resalePrices.add(Double.parseDouble(values[9].trim()));
                indexSet.add(lineCount);
                lineCount++;
            }

        }
        System.out.println("Data loaded successfully. Total records: " + lineCount);
    }


    public void filterData(String townName, String startYearMonth, String endYearMonth, Integer area) {
        System.out.println("Filtering data for town: " + townName);
        System.out.println("Start Year Month: " + startYearMonth);
        System.out.println("End Year Month: " + endYearMonth);
        System.out.println("Floor Area: " + area);
        System.out.println("Index Set: " + indexSet.size());

        // Remove all indices that do not match the filter criteria (Ang Mo Kio)
        indexSet.removeIf(i -> !towns.get(i).equals(townName));
        indexSet.removeIf(i -> !months.get(i).equals(startYearMonth) && !months.get(i).equals(endYearMonth));
        indexSet.removeIf(i -> floorAreas.get(i) < area);

        System.out.println("Index Set after start filter: " + indexSet.size());
    }

    // Return out an array of resalesPrices
    public List<Double> getResalePrices() {

        List<Double> filteredResalePrices = new ArrayList<>();
        for (Integer index : indexSet) {
            filteredResalePrices.add(resalePrices.get(index));
        }
        return filteredResalePrices;
    }
}
