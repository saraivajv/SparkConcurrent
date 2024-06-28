package model;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class TxtToAvroConverter {

    private static final String INPUT_TXT_PATH = "C:\\Users\\joaov\\git\\textao.txt";
    private static final String OUTPUT_AVRO_PATH = "C:\\Users\\joaov\\git\\textao.avro";

    public static void main(String[] args) {
        SparkSession spark = SparkSession
            .builder()
            .appName("TxtToAvroConverter")
            .master("local[*]")
            .config("spark.driver.extraJavaOptions", "--add-opens java.base/java.nio=ALL-UNNAMED --add-opens java.base/java.lang=ALL-UNNAMED")
            .config("spark.executor.extraJavaOptions", "--add-opens java.base/java.nio=ALL-UNNAMED --add-opens java.base/java.lang=ALL-UNNAMED")
            .getOrCreate();

        // Read the TXT file as a DataFrame
        Dataset<Row> textDF = spark.read().format("text").load(INPUT_TXT_PATH).toDF("line");

        // Save the DataFrame as an Avro file
        textDF.write().format("avro").save(OUTPUT_AVRO_PATH);

        spark.stop();
    }
}
