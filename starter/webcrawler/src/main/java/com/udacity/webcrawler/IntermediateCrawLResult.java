package com.udacity.webcrawler;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class IntermediateCrawLResult {
    private final Map<String, Integer> wordCounts = new ConcurrentHashMap<>();
    private int pageCount = 0;

    public synchronized void updateResult(Map<String, Integer> newWordCounts, int pageCount){
        for (Map.Entry<String, Integer> e : newWordCounts.entrySet()) {
            if (wordCounts.containsKey(e.getKey())) {
                wordCounts.put(e.getKey(), e.getValue() + wordCounts.get(e.getKey()));
            } else {
                wordCounts.put(e.getKey(), e.getValue());
            }
        }
        this.pageCount += pageCount;
    }
    public Map<String, Integer> getWordCounts(){
        return wordCounts;
    }

    public int getPageCount(){
        return pageCount;
    }
}