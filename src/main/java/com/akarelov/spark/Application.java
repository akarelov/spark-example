package com.akarelov.spark;

import com.akarelov.spark.service.PopularWordsService;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@SpringBootApplication
@RestController
public class Application {
    private final JavaSparkContext ctx;
    private final PopularWordsService popularWordsService;

    public Application(JavaSparkContext ctx, PopularWordsService popularWordsService) {
        this.ctx = ctx;
        this.popularWordsService = popularWordsService;
    }

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }

    @GetMapping(value = "/")
    public List<String> getWords(@RequestParam (value = "amount") Integer amount) {
        JavaRDD<String> lines = ctx.textFile("src/main/resources/song", 1);
        return popularWordsService.topX(lines, amount);
    }
}
