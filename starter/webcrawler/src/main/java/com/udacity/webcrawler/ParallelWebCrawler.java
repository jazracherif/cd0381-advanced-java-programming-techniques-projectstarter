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
    IntermediateCrawLResult intermediateResults = new IntermediateCrawLResult();

    for (String url: startingUrls) {
      ForkJoinTask task = new CrawlerRecursiveAction.builder()
              .setUrl(url)
              .setParserFactory(parserFactory)
              .setGlobalVisitedUrl(globalVisitedUrl)
              .setIgnoredUrls(ignoredUrls)
              .setClock(clock)
              .setDeadline(deadline)
              .setMaxDepth(maxDepth)
              .setIntermediateCrawLResult(intermediateResults)
              .build();
      this.pool.execute(task);
    }

    try {
      this.pool.shutdown();
      this.pool.awaitTermination(100, TimeUnit.SECONDS);
    } catch (Exception e){
      e.printStackTrace();
    }

    // Merge the results from all the visited Urls
    return new CrawlResult.Builder()
            .setWordCounts(WordCounts.sort(intermediateResults.getWordCounts(), popularWordCount))
            .setUrlsVisited(intermediateResults.getPageCount())
            .build();
  }

  @Override
  public int getMaxParallelism() {
    return Runtime.getRuntime().availableProcessors();
  }
}
