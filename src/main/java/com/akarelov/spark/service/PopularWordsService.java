package com.akarelov.spark.service;

import org.apache.spark.api.java.JavaRDD;
import org.springframework.stereotype.Service;

import java.io.Serializable;
import java.util.List;

@Service
public interface PopularWordsService extends Serializable {
    List<String> topX(JavaRDD<String> lines, int x);
}
