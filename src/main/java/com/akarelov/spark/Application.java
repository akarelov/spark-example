package com.akarelov.spark;

import com.akarelov.spark.configuration.AppConfig;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import scala.Tuple2;

import java.io.File;
import java.util.Arrays;
import java.util.List;

import static org.apache.commons.lang3.StringUtils.SPACE;

@SpringBootApplication
@RestController
public class Application {
    private final JavaSparkContext ctx;

    public Application(JavaSparkContext ctx) {
        this.ctx = ctx;
    }

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }

    @GetMapping
    public ResponseEntity<List<Tuple2<String,Integer>>> getWords(@RequestParam(value = "text") String text) {

//        if (args.length < 1) {
//            System.exit(1);
//        }

//        JavaRDD<String> lines = ctx.textFile(args[0], 1);
        File file = new File("src/main/resources/song");
        JavaRDD<String> lines = ctx.textFile("src/main/resources/song", 1);
        JavaRDD<String> words
                = lines.flatMap(s -> Arrays.asList(SPACE.split(s)).iterator());
        JavaPairRDD<String, Integer> ones
                = words.mapToPair(word -> new Tuple2<>(word, 1));
        JavaPairRDD<String, Integer> counts
                = ones.reduceByKey((Integer i1, Integer i2) -> i1 + i2);

        List<Tuple2<String, Integer>> output = counts.collect();
        for (Tuple2<?, ?> tuple : output) {
            System.out.println(tuple._1() + ": " + tuple._2());
        }
        return new ResponseEntity<>(output, HttpStatus.OK);
    }
}
