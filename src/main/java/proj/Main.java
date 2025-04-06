package proj;
import java.util.HashMap;
import java.util.Map;

public class Main {

    public static void main(String[] args) {

        // Get argument input from the user
        if (args.length == 0) {
            System.out.println("Please provide the Matric Card as an argument.");
            return;
        }

        Map<String, String> result = preprocess(args[0]);
        ColumnStore cs = new ColumnStore();

        try {

            cs.loadData(Constant.FILEPATH);
            cs.filterData(result.get(Constant.KEY_TOWN_NAME), result.get(Constant.KEY_START_YEAR_MONTH), result.get(Constant.KEY_END_YEAR_MONTH), Constant.AREA);

        } catch (Exception e) {
            System.out.println("Error loading data: " + e.getMessage());
        }



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