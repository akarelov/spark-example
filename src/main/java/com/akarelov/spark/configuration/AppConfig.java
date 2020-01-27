package com.akarelov.spark.configuration;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class AppConfig {

    @Bean
    public JavaSparkContext sc() {
        SparkConf sparkConf = new SparkConf().setAppName("JavaWordCount").setMaster("local[*]");
        return new JavaSparkContext(sparkConf);
    }
}
