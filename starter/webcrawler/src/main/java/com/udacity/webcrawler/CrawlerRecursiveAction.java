package com.udacity.webcrawler;

import com.udacity.webcrawler.parser.PageParser;
import com.udacity.webcrawler.parser.PageParserFactory;

import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.RecursiveAction;
import java.util.regex.Pattern;

public class CrawlerRecursiveAction extends RecursiveAction {
    private final String url;
    private final PageParserFactory parserFactory;
    private final Set<String> globalVisitedUrl;
    private final Clock clock;
    private final Instant deadline;
    private final int maxDepth;
    private final List<Pattern> ignoredUrls;

    private final IntermediateCrawLResult intermediateCrawLResult;

    private CrawlerRecursiveAction(String urls,
                                   PageParserFactory parserFactory,
                                   Set<String> globalVisitedUrl,
                                   Clock clock,
                                   int maxDepth,
                                   Instant deadline,
                                   List<Pattern> ignoredUrls,
                                   IntermediateCrawLResult intermediateCrawLResult){
        this.url = urls;
        this.parserFactory = parserFactory;
        this.globalVisitedUrl = globalVisitedUrl;
        this.clock = clock;
        this.maxDepth = maxDepth;
        this.deadline = deadline;
        this.ignoredUrls = ignoredUrls;
        this.intermediateCrawLResult = intermediateCrawLResult;
    }

    @Override
    protected void compute() {

        // Check whether we should keep crawling
        if (maxDepth == 0 || clock.instant().isAfter(deadline)) {
            return;
        }

        for (Pattern pattern : ignoredUrls) {
            if (pattern.matcher(url).matches()) {
                return;
            }
        }

        synchronized (globalVisitedUrl) {
            if (globalVisitedUrl.contains(url))
                // Already Visited
                return;

            globalVisitedUrl.add(url);
        }

        // Parse current page
        PageParser.Result result = parserFactory.get(url).parse();

        // Launch new parallel tasks to process links in the current page
        List<RecursiveAction> tasks = new ArrayList<>();
        for (String subLink: result.getLinks()) {
            tasks.add(new CrawlerRecursiveAction.builder()
                    .setUrl(subLink).setParserFactory(parserFactory)
                    .setParserFactory(parserFactory)
                    .setGlobalVisitedUrl(globalVisitedUrl)
                    .setIgnoredUrls(ignoredUrls)
                    .setClock(clock)
                    .setDeadline(deadline)
                    .setMaxDepth(maxDepth - 1)
                    .setIntermediateCrawLResult(intermediateCrawLResult)
                    .build());
        }
        invokeAll(tasks);

        // Update intermediary results from current page information
        intermediateCrawLResult.updateResult(result.getWordCounts(), 1);
    }

    static final class builder {
        private String url;
        private PageParserFactory parserFactory;
        private Set<String> globalVisitedUrl = new HashSet<>();
        private Clock clock;
        private Instant deadline;
        private int maxDepth;
        private List<Pattern> ignoredUrls = new ArrayList<>();
        private IntermediateCrawLResult intermediateCrawLResult;

        public builder setUrl(String url){
            this.url = url;
            return this;
        }

        public builder setParserFactory(PageParserFactory parserFactory){
            this.parserFactory = parserFactory;
            return this;
        }

        public builder setGlobalVisitedUrl(Set<String> globalVisitedUrl){
            this.globalVisitedUrl = globalVisitedUrl;
            return this;
        }

        public builder setClock(Clock clock){
            this.clock = clock;
            return this;
        }

        public builder setDeadline(Instant deadline){
            this.deadline = deadline;
            return this;
        }

        public builder setMaxDepth(int maxDepth){
            this.maxDepth = maxDepth;
            return this;
        }

        public builder setIgnoredUrls(List<Pattern> ignoredUrls){
            this.ignoredUrls = ignoredUrls;
            return this;
        }

        public builder setIntermediateCrawLResult(IntermediateCrawLResult intermediateCrawLResult){
            this.intermediateCrawLResult = intermediateCrawLResult;
            return this;
        }

        public CrawlerRecursiveAction build(){
            return new CrawlerRecursiveAction(
                    url,
                    parserFactory,
                    globalVisitedUrl,
                    clock,
                    maxDepth,
                    deadline,
                    ignoredUrls,
                    intermediateCrawLResult);
        }
    }
}
