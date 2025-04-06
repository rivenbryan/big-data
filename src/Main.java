import java.util.HashMap;
import java.util.Map;

public class Main {
    public static void main(String[] args) {
        // Get argument input from the user
        if (args.length == 0) {
            System.out.println("Please provide the Matric Card as an argument.");
            return;
        }
        // Get the output from preprocess
        Map<String, String> result = preprocess(args[0]);

        // Print out result
        System.out.println("Town Name: " + result.get(Constant.KEY_TOWN_NAME));
        System.out.println("Start Year Month: " + result.get(Constant.KEY_START_YEAR_MONTH));
        System.out.println("End Year Month: " + result.get(Constant.KEY_END_YEAR_MONTH));







        // Extract out third last digit

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