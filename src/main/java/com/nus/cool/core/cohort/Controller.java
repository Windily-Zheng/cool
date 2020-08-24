package com.nus.cool.core.cohort;

import com.nus.cool.core.iceberg.query.IcebergQuery;
import com.nus.cool.core.iceberg.result.BaseResult;
import com.nus.cool.core.io.cache.CacheManager;
import com.nus.cool.loader.CohortLoader;
import com.nus.cool.loader.CoolModel;
import com.nus.cool.loader.IcebergLoader;
import com.nus.cool.loader.ResultTuple;
import java.io.IOException;
import java.text.ParseException;
import java.util.List;

public class Controller {

  private static CoolModel coolModel;

  private static CacheManager cacheManager;

  private static CohortLoader cohortLoader;

  private static IcebergLoader icebergLoader;

  private static CohortProcessor cohortProcessor;

  private static IcebergProcessor icebergProcessor;

  public static void main(String[] args) throws IOException, ParseException {
    coolModel = new CoolModel(args[0]);
    coolModel.reload(args[1]);

    double memoryCacheSize = (double) 20 * 1024 * 1024 * 1024;
    double diskCacheSize = (double) 40 * 1024 * 1024 * 1024;
    cacheManager = new CacheManager(args[0] + "/" + args[2], memoryCacheSize, diskCacheSize,
        0.8);

    // TODO: Need to get from query
    String queryType = "Iceberg";

    if ("Iceberg".equals(queryType)) {
      icebergLoader = new IcebergLoader();
      icebergProcessor = new IcebergProcessor();
      List<IcebergQuery> queries = icebergLoader.load();

      for (IcebergQuery query : queries) {
        List<BaseResult> results = icebergProcessor
            .executeQuery(coolModel.getCube(query.getDataSource()), query, cacheManager);
        QueryResult result = QueryResult.ok(results);
        System.out.println(result.toString());

        cacheManager.caching();
      }

    } else if ("Cohort".equals(queryType)) {
      cohortLoader = new CohortLoader();
      cohortProcessor = new CohortProcessor();
      List<CohortQuery> queries = cohortLoader.load();

      long cachingTime = 0;
      long queryTime = 0;
//    int count = 1;

      for (CohortQuery query : queries) {
        long queryStart = System.nanoTime();
        List<ResultTuple> resultTuples = cohortProcessor
            .executeQuery(coolModel.getCube(query.getDataSource()), query, cacheManager);
        long queryEnd = System.nanoTime();
        queryTime += (queryEnd - queryStart);

//      System.out.println("Q[" + count + "]: ");
//      System.out.printf("%.3f ms\n", (double)(queryEnd - queryStart) / 1000000);

        long cachingStart = System.nanoTime();
        cacheManager.caching();
        long cachingEnd = System.nanoTime();
        cachingTime += (cachingEnd - cachingStart);

//      QueryResult result = QueryResult.ok(resultTuples);
//      System.out.println("Q[" + count + "]: ");
//      System.out.println(result.toString());
//      count++;
      }

      double aveQueryTime = queryTime / queries.size();
//    double aveSeekTime = CohortProcessor.totalSeekTime / queries.size();
//    double aveSelectionTime = CohortProcessor.totalSelectionTime / queries.size();
//    double aveLoadTime = CohortProcessor.totalLoadTime / queries.size();
//    double aveGenerateTime = CohortProcessor.totalGenerateTime / queries.size();
      double aveFilterTime = CohortProcessor.totalFilterTime / queries.size();
//    double aveCachingTime = cachingTime / queries.size();
      double aveSelectionTime =
          CohortProcessor.totalSeekTime + CohortProcessor.totalSelectionTime / queries.size();
      double aveAggregationTime = CohortProcessor.totalAggregationTime / queries.size();

      System.out
          .printf("Average Query Time: %.2f ns => %.3f ms\n", aveQueryTime, aveQueryTime / 1000000);
//    System.out
//        .printf("Average Seek Time: %.2f ns => %.3f ms\n", aveSeekTime, aveSeekTime / 1000000);
//    System.out.printf("Average Age Selection Time: %.2f ns => %.3f ms\n", aveSelectionTime,
//        aveSelectionTime / 1000000);
//    System.out
//        .printf("Average Load Time: %.2f ns => %.3f ms\n", aveLoadTime, aveLoadTime / 1000000);
//    System.out.printf("Average Generate Time: %.2f ns => %.3f ms\n", aveGenerateTime,
//        aveGenerateTime / 1000000);
      System.out.printf("Average Filter Time: %.2f ns => %.3f ms\n", aveFilterTime,
          aveFilterTime / 1000000);
//    System.out.printf("Average Caching Time: %.2f ns => %.3f ms\n", aveCachingTime,
//        aveCachingTime / 1000000);
      System.out.printf("Average Selection Time: %.2f ns => %.3f ms\n", aveSelectionTime,
          aveSelectionTime / 1000000);
      System.out.printf("Average Aggregation Time: %.2f ns => %.3f ms\n", aveAggregationTime,
          aveAggregationTime / 1000000);
      System.out
          .printf("Hit rate: %.2f%%\n",
              CacheManager.getHitNum() / CacheManager.getTotalNum() * 100);
    }

    coolModel.close();

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
  }
}
