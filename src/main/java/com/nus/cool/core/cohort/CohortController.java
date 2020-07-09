package com.nus.cool.core.cohort;

import com.nus.cool.core.io.cache.CacheManager;
import com.nus.cool.loader.CohortLoader;
import com.nus.cool.loader.CoolModel;
import com.nus.cool.loader.ResultTuple;
import java.io.IOException;
import java.util.List;

public class CohortController {

  private static CoolModel coolModel;

  private static CohortLoader cohortLoader;

  private static CacheManager cacheManager;

  private static CohortProcessor cohortProcessor;

  public static void main(String[] args) throws IOException {
    coolModel = new CoolModel(args[0]);
    coolModel.reload(args[1]);

    cohortLoader = new CohortLoader();

    double memoryCacheSize = (double) 20 * 1024 * 1024 * 1024;
    double diskCacheSize = (double) 40 * 1024 * 1024 * 1024;

    cacheManager = new CacheManager(args[0] + "/" + args[2], memoryCacheSize, diskCacheSize,
        0.8);

    cohortProcessor = new CohortProcessor();

    List<CohortQuery> queries = cohortLoader.load();

//    Map<String, DataOutputStream> map = Maps.newHashMap();
//    if (query.getOutSource() != null) {
//      File root = new File("cube/", query.getOutSource());
//      File[] versions = root.listFiles(new FileFilter() {
//        @Override
//        public boolean accept(File file) {
//          return file.isDirectory();
//        }
//      });
//      for (File version : versions) {
//        File[] cubletFiles = version.listFiles(new FilenameFilter() {
//          @Override
//          public boolean accept(File file, String s) {
//            return s.endsWith(".dz");
//          }
//        });
//        for (File cubletFile : cubletFiles) {
//          map.put(cubletFile.getName(),
//              new DataOutputStream(new FileOutputStream(cubletFile, true)));
//        }
//      }
//    }

//    long totalTime = 0;
//    int count = 0;

    long cachingTime = 0;
    for (CohortQuery query : queries) {
//      long startTime = System.currentTimeMillis();
//      long startTime = System.nanoTime();

      List<ResultTuple> resultTuples = cohortProcessor
          .executeQuery(coolModel.getCube(query.getDataSource()), query, cacheManager);

      long cachingStart = System.nanoTime();
      cacheManager.caching();
      long cachingEnd = System.nanoTime();
      cachingTime += (cachingEnd - cachingStart);

//      long endTime = System.currentTimeMillis();
//      long endTime = System.nanoTime();
//      long queryTime = endTime - startTime;
//      totalTime += queryTime;
//      count++;
//      System.out.println("Q[" + count + "]: " + queryTime + " ns");
//      System.out.println("Q[" + count + "]: " + queryTime + " ms");

//      QueryResult result = QueryResult.ok(resultTuples);
//      System.out.println(result.toString());
    }

//    System.out.println("Total time: " + totalTime + " ns");
//    System.out.print("Average time: " + totalTime * 1.0 / count + " ns => ");
//    System.out.printf("%.3f ms\n", totalTime * 1.0 / count / 1000000);
//    System.out.println("Total time: " + totalTime + " ms");
//    System.out.println("Average time: " + totalTime * 1.0 / count + " ms");
    double aveSeekTime = CohortProcessor.totalSeekTime / queries.size();
    double aveSelectionTime = CohortProcessor.totalSelectionTime / queries.size();
    double aveLoadTime = CohortProcessor.totalLoadTime / queries.size();
    double aveGenerateTime = CohortProcessor.totalGenerateTime / queries.size();
    double aveFilterTime = CohortProcessor.totalFilterTime / queries.size();
    double aveCachingTime = cachingTime / queries.size();

    System.out
        .printf("Average Seek Time: %.2f ns => %.3f ms\n", aveSeekTime, aveSeekTime / 1000000);
    System.out.printf("Average Age Selection Time: %.2f ns => %.3f ms\n", aveSelectionTime,
        aveSelectionTime / 1000000);
    System.out
        .printf("Average Load Time: %.2f ns => %.3f ms\n", aveLoadTime, aveLoadTime / 1000000);
    System.out.printf("Average Generate Time: %.2f ns => %.3f ms\n", aveGenerateTime,
        aveGenerateTime / 1000000);
    System.out.printf("Average Filter Time: %.2f ns => %.3f ms\n", aveFilterTime,
        aveFilterTime / 1000000);
    System.out.printf("Average Caching Time: %.2f ns => %.3f ms\n", aveCachingTime,
        aveCachingTime / 1000000);
    System.out
        .printf("Hit rate: %.2f%%\n", CacheManager.getHitNum() / CacheManager.getTotalNum() * 100);
    coolModel.close();
  }
}
