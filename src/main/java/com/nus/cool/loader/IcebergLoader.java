/*
 * Copyright 2020 Cool Squad Team
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.nus.cool.loader;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.nus.cool.core.iceberg.query.IcebergQuery;
import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * @author hongbin
 * @version 0.1
 * @since 0.1
 */
public class IcebergLoader {

//  public static QueryResult wrapResult(CubeRS cube, IcebergQuery query) {
//    try {
//      List<BaseResult> results = executeQuery(cube, query);
//      return QueryResult.ok(results);
//    } catch (Exception e) {
//      e.printStackTrace();
//    }
//    return null;
//  }

  public List<IcebergQuery> load() throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    List<IcebergQuery> queries = Lists.newArrayList();

//    String queryRoot = "/Users/zhiyi/Desktop/Cool/cool/iceberg-query/";
    String queryRoot = "/home/zju/zhengzhiyi/Cool/cool/iceberg-query/";

    for (int i = 1; i <= 10; i++) {
      String fileName = queryRoot + "query" + i + ".json";
      IcebergQuery query = mapper.readValue(new File(fileName), IcebergQuery.class);
      queries.add(query);
    }

//    String fileName = queryRoot + "query1.json";
//    IcebergQuery query = mapper.readValue(new File(fileName), IcebergQuery.class);
//    queries.add(query);

    return queries;
//    QueryResult result = wrapResult(coolModel.getCube(query.getDataSource()), query);
  }
}
