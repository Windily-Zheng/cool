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

    cacheManager = new CacheManager(args[0] + "/" + args[2], 3000, 0.6);

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

    for (CohortQuery query : queries) {
      List<ResultTuple> resultTuples = cohortProcessor
          .executeQuery(coolModel.getCube(query.getDataSource()), query, cacheManager);
      QueryResult result = QueryResult.ok(resultTuples);
      System.out.println(result.toString());
    }
    coolModel.close();
  }

}
