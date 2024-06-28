package sparkproject;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class LevenshteinDistanceSparkSQL {

    private static final String DATASET_PATH = "C:\\Users\\joaov\\git\\textao.txt";
    private static final String REFERENCE_WORD = "tour";
    private static final int MAX_DISTANCE = 3;

    public static void main(String[] args) {
        SparkSession spark = SparkSession
            .builder()
            .appName("LevenshteinDistanceSparkSQL")
            .master("local[*]")
            .config("spark.driver.extraJavaOptions", "--add-opens java.base/java.nio=ALL-UNNAMED --add-opens java.base/java.lang=ALL-UNNAMED")
            .config("spark.executor.extraJavaOptions", "--add-opens java.base/java.nio=ALL-UNNAMED --add-opens java.base/java.lang=ALL-UNNAMED")
            .getOrCreate();

        // Read the dataset as a DataFrame
        Dataset<Row> linesDF = spark.read().text(DATASET_PATH).toDF("line");

        // Split lines into words and explode into rows
        Dataset<Row> wordsDF = linesDF
            .select(explode(split(col("line"), "\\s+")).alias("word"));

        // Create or replace a temporary view to use SQL queries
        wordsDF.createOrReplaceTempView("words");

        // Calculate Levenshtein distances using SQL
        Dataset<Row> similarWordsDF = spark.sql(
            "SELECT word FROM words WHERE " +
            "levenshtein(lower(word), '" + REFERENCE_WORD + "') <= " + MAX_DISTANCE
        );

        // Count the number of similar words
        long totalSimWords = similarWordsDF.count();

        System.out.println("Quantidade de palavras parecidas encontradas: " + totalSimWords);
        
        // Manter o Spark UI disponível após a finalização do programa
        System.out.println("Pressione Enter para finalizar o programa...");
        try {
            System.in.read();
        } catch (Exception e) {
            e.printStackTrace();
        }

        spark.stop();
    }
}
