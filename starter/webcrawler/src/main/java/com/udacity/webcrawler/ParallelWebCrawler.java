package com.udacity.webcrawler;

import com.udacity.webcrawler.json.CrawlResult;
import com.udacity.webcrawler.parser.PageParser;
import com.udacity.webcrawler.parser.PageParserFactory;

import javax.inject.Inject;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.regex.Pattern;

/**
 * A concrete implementation of {@link WebCrawler} that runs multiple threads on a
 * {@link ForkJoinPool} to fetch and process multiple web pages in parallel.
 */
final class ParallelWebCrawler implements WebCrawler {
  private final Clock clock;
  private final Duration timeout;

  private final int maxDepth;

  private final List<Pattern> ignoredUrls;

  private final int popularWordCount;

  private final PageParserFactory parserFactory;
  private final ForkJoinPool pool;

  // needs Sync Access
  private final Set<String> globalVisitedUrl = Collections.synchronizedSet(new HashSet<>());

   static final class CrawlerRecursiveAction extends RecursiveTask<CrawlResult> {
    private final String url;
    private final PageParserFactory parserFactory;
    private final Set<String> globalVisitedUrl;
    private final Clock clock;
    private final Instant deadline;
    private final int maxDepth;
    private final List<Pattern> ignoredUrls;

    private final Map<String, Integer> wordCounts;
    private int pageCount;

    private CrawlerRecursiveAction(String urls,
                                   PageParserFactory parserFactory,
                                   Set<String> globalVisitedUrl,
                                   Clock clock,
                                   int maxDepth,
                                   Instant deadline,
                                   List<Pattern> ignoredUrls){
      this.url = urls;
      this.parserFactory = parserFactory;
      this.globalVisitedUrl = globalVisitedUrl;
      this.clock = clock;
      this.maxDepth = maxDepth;
      this.deadline = deadline;
      this.ignoredUrls = ignoredUrls;

      this.wordCounts = new HashMap<>();
      this.pageCount = 0;

    }

    private void updateFromParentLink(PageParser.Result result){
      ParallelWebCrawler.mergeWordCounts(wordCounts, result.getWordCounts());
      this.pageCount += 1;
    }

    private void updateFromChildrenLink(Collection<RecursiveTask<CrawlResult>> subResults){
      for (RecursiveTask<CrawlResult> subResult: subResults){
        if (subResult.isDone()){
          try {
            ParallelWebCrawler.mergeWordCounts(wordCounts, subResult.get().getWordCounts());
            pageCount += subResult.get().getUrlsVisited();
          } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
          }
        }
      }
    }

    @Override
    protected CrawlResult compute() {
      if (maxDepth == 0 || clock.instant().isAfter(deadline)) {
        return new CrawlResult.Builder().setWordCounts(new HashMap<>()).setUrlsVisited(0).build();
      }

      for (Pattern pattern : ignoredUrls) {
        if (pattern.matcher(url).matches()) {
          return new CrawlResult.Builder().setWordCounts(new HashMap<>()).setUrlsVisited(0).build();
        }
      }

      synchronized (globalVisitedUrl) {
        if (globalVisitedUrl.contains(url))
          // Already Visited
          return new CrawlResult.Builder().setWordCounts(new HashMap<>()).setUrlsVisited(0).build();

        globalVisitedUrl.add(url);
      }

      // Get Current Page information
      PageParser.Result result = parserFactory.get(url).parse();
      updateFromParentLink(result);

      // Create all parallel subtasks
      List<RecursiveTask<CrawlResult>> tasks = new ArrayList<>();
      for (String subLink: result.getLinks()) {
        tasks.add(new CrawlerRecursiveAction.builder()
                .setUrl(subLink).setParserFactory(parserFactory)
                .setParserFactory(parserFactory)
                .setGlobalVisitedUrl(globalVisitedUrl)
                .setIgnoredUrls(ignoredUrls)
                .setClock(clock)
                .setDeadline(deadline)
                .setMaxDepth(maxDepth - 1)
                .build());
      }

      // Invoke subtask and update results
      updateFromChildrenLink(invokeAll(tasks));

      return new CrawlResult.Builder().setWordCounts(wordCounts).setUrlsVisited(pageCount).build();
    }

    static final class builder {
      private String url;
      private PageParserFactory parserFactory;

      private Set<String> globalVisitedUrl = new HashSet<>();
      private Clock clock;
      private Instant deadline;
      private int maxDepth;
      private List<Pattern> ignoredUrls = new ArrayList<>();

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

      public CrawlerRecursiveAction build(){
        return new CrawlerRecursiveAction(
                url,
                parserFactory,
                globalVisitedUrl,
                clock,
                maxDepth,
                deadline,
                ignoredUrls);
      }
    }
  }
  @Inject
  ParallelWebCrawler(
      Clock clock,
      PageParserFactory parserFactory,
      @Timeout Duration timeout,
      @MaxDepth int maxDepth,
      @PopularWordCount int popularWordCount,
      @IgnoredUrls List<Pattern> ignoredUrls,
      @TargetParallelism int threadCount) {
    this.clock = clock;
    this.parserFactory = parserFactory;
    this.timeout = timeout;
    this.popularWordCount = popularWordCount;
    this.maxDepth = maxDepth;
    this.ignoredUrls = ignoredUrls;

    this.pool = new ForkJoinPool(Math.min(threadCount, getMaxParallelism()));
  }

  @Override
  public CrawlResult crawl(List<String> startingUrls) {

    Instant deadline = clock.instant().plus(timeout);

    // Create parallel tasks needed to crawl through the starting urls
    Collection<ForkJoinTask<CrawlResult>> tasks = new ArrayList<>();
    for (String url: startingUrls) {
      ForkJoinTask<CrawlResult> task = new CrawlerRecursiveAction.builder()
              .setUrl(url)
              .setParserFactory(parserFactory)
              .setGlobalVisitedUrl(globalVisitedUrl)
              .setIgnoredUrls(ignoredUrls)
              .setClock(clock)
              .setDeadline(deadline)
              .setMaxDepth(maxDepth)
              .build();
      this.pool.execute(task);
      tasks.add(task);
    }

    // Start Crawling
    for (ForkJoinTask<CrawlResult> task: tasks)
      try {
        task.join();
      } catch (Exception e){
        e.printStackTrace();
      }

    // Merge the results from all the visited Urls
    return combineCrawlResults(tasks);
  }

  private CrawlResult combineCrawlResults(Collection<ForkJoinTask<CrawlResult>> tasks){
     // Combine results from all tasks
     Map<String, Integer> wordCounts = new HashMap<>();
     int urlVisited = 0;
     for (ForkJoinTask<CrawlResult> task: tasks){
       if (task.isDone()){
         try {
           mergeWordCounts(wordCounts, task.get().getWordCounts());
           urlVisited += task.get().getUrlsVisited();
         } catch (Exception e){
           e.printStackTrace();
         }
       }
     }

     // merge results
     return new CrawlResult.Builder()
            .setWordCounts(wordCounts.isEmpty() ? wordCounts : WordCounts.sort(wordCounts, popularWordCount))
            .setUrlsVisited(urlVisited)
            .build();
  }
  public static void mergeWordCounts(Map<String, Integer> dst, Map<String, Integer> next){
      for (Map.Entry<String, Integer> e : next.entrySet()) {
        if (dst.containsKey(e.getKey())) {
          dst.put(e.getKey(), e.getValue() + dst.get(e.getKey()));
        } else {
          dst.put(e.getKey(), e.getValue());
        }
    }
  }

  @Override
  public int getMaxParallelism() {
    return Runtime.getRuntime().availableProcessors();
  }
}
