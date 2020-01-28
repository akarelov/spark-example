package com.akarelov.spark.service;

import com.akarelov.spark.service.PopularWordsService;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaRDD;
import org.springframework.stereotype.Service;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

@Service
public class PopularWordsServiceImpl implements PopularWordsService {
    @Override
    public List<String> topX(JavaRDD<String> lines, int x) {
        return lines.map(String::toLowerCase)
                .flatMap(this::getWords)
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey(Integer::sum)
                .mapToPair(Tuple2::swap)
                .sortByKey(false)
                .map(Tuple2::_2)
                .take(x);
    }

    public Iterator<String> getWords(String str) {
        return Arrays.asList(str.split("[0-9&.,?!;:'\\s\\[\\]\\)\\(\"]+")).iterator();
    }
}