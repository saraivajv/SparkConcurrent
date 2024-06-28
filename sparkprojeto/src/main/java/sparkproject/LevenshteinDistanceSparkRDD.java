package sparkproject;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.SparkConf;

import java.util.Arrays;

public class LevenshteinDistanceSparkRDD {

    private static final String DATASET_PATH = "C:\\Users\\joaov\\git\\textao.txt";
    private static final String REFERENCE_WORD = "tour";
    private static final int MAX_DISTANCE = 3;

    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
            .setAppName("LevenshteinDistanceSpark")
            .setMaster("local[*]")
            .set("spark.driver.extraJavaOptions", "--add-opens java.base/java.nio=ALL-UNNAMED --add-opens java.base/java.lang=ALL-UNNAMED")
            .set("spark.executor.extraJavaOptions", "--add-opens java.base/java.nio=ALL-UNNAMED --add-opens java.base/java.lang=ALL-UNNAMED");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Read the dataset directly as an RDD
        JavaRDD<String> wordsRDD = sc.textFile(DATASET_PATH)
                                     .flatMap(line -> Arrays.asList(line.split("\\s+")).iterator());

        // Calculate Levenshtein distances and filter similar words
        JavaRDD<String> similarWordsRDD = wordsRDD.filter(word -> 
            calculateLevenshteinDistance(REFERENCE_WORD, word.toLowerCase()) <= MAX_DISTANCE
        );

        // Count the number of similar words
        long totalSimWords = similarWordsRDD.count();

        System.out.println("Quantidade de palavras parecidas encontradas: " + totalSimWords);
        
     // Manter o Spark UI disponível após a finalização do programa
        System.out.println("Pressione Enter para finalizar o programa...");
        try {
            System.in.read();
        } catch (Exception e) {
            e.printStackTrace();
        }

        sc.close();
    }

    public static int calculateLevenshteinDistance(String word1, String word2) {
        int[][] dp = new int[word1.length() + 1][word2.length() + 1];

        for (int i = 0; i <= word1.length(); i++) {
            for (int j = 0; j <= word2.length(); j++) {
                if (i == 0) {
                    dp[i][j] = j;
                } else if (j == 0) {
                    dp[i][j] = i;
                } else {
                    dp[i][j] = min(dp[i - 1][j - 1] + costOfSubstitution(word1.charAt(i - 1), word2.charAt(j - 1)),
                            dp[i - 1][j] + 1,
                            dp[i][j - 1] + 1);
                }
            }
        }

        return dp[word1.length()][word2.length()];
    }

    private static int costOfSubstitution(char a, char b) {
        return a == b ? 0 : 1;
    }

    private static int min(int... numbers) {
        return Math.min(numbers[0], Math.min(numbers[1], numbers[2]));
    }
}
