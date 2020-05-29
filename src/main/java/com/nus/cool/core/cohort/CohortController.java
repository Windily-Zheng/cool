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

    cacheManager = new CacheManager(args[0] + "/" + args[2], 3000, 5000, 0.8);

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

    long totalTime = 0;

    for (CohortQuery query : queries) {
      long startTime = System.currentTimeMillis();

      List<ResultTuple> resultTuples = cohortProcessor
          .executeQuery(coolModel.getCube(query.getDataSource()), query, cacheManager);

      long endTime = System.currentTimeMillis();
      long queryTime = endTime - startTime;
      totalTime += queryTime;
      System.out.println("response time: " + queryTime + "ms");

      QueryResult result = QueryResult.ok(resultTuples);
      System.out.println(result.toString());
    }

    System.out.println("Total time: " + totalTime + "ms");
    coolModel.close();
  }

}
