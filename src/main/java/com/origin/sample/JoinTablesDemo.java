package com.origin.sample;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.ReadConfig;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;

public class JoinTablesDemo {
	public static void main(final String[] args) throws InterruptedException {

		SparkSession spark = SparkSession.builder().master("local").appName("MongoSparkConnectorIntro")
				.config("spark.mongodb.input.uri", "mongodb://127.0.0.1/test.characters")
				.config("spark.mongodb.output.uri", "mongodb://127.0.0.1/test.characters")
				.config("spark.sql.warehouse.dir", "file:///C:/path/to/my/")
				.config("spark.sql.crossJoin.enabled", "true").config("spark.driver.host", "localhost").getOrCreate();

		// Create a JavaSparkContext using the SparkSession's SparkContext object
		JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

		// Load data and infer schema, disregard toDF() name as it returns Dataset
		Dataset<Row> implicitDS = MongoSpark.load(jsc).toDF();
		implicitDS.printSchema();

		// Load data with explicit schema
		Dataset<Character> charectersDS = MongoSpark.load(jsc).toDS(Character.class);
		charectersDS.printSchema();
		// explicitDS.show();

		// Create the temp view and execute the query
		charectersDS.createOrReplaceTempView("characters");

		Map<String, String> readOverrides = new HashMap<String, String>();
		readOverrides.put("collection", "spark");
		readOverrides.put("readPreference.name", "secondaryPreferred");
		ReadConfig readConfig = ReadConfig.create(jsc).withOptions(readOverrides);
		JavaMongoRDD<Document> customRdd = MongoSpark.load(jsc, readConfig);

		Dataset<Row> sparkDS = customRdd.toDF();
		sparkDS.show();
		System.out.println("+++++++++++++++++++++++++++++++++++++++++++++");
		Dataset<Row> joined = charectersDS.join(sparkDS, charectersDS.col("age").equalTo("1000"));

		joined.show();

		jsc.close();

	}
}
