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
import com.nus.cool.core.cohort.QueryResult;
import com.nus.cool.core.iceberg.query.Aggregation;
import com.nus.cool.core.iceberg.query.IcebergAggregation;
import com.nus.cool.core.iceberg.query.IcebergQuery;
import com.nus.cool.core.iceberg.query.IcebergSelection;
import com.nus.cool.core.iceberg.result.BaseResult;
import com.nus.cool.core.io.readstore.ChunkRS;
import com.nus.cool.core.io.readstore.CubeRS;
import com.nus.cool.core.io.readstore.CubletRS;
import com.nus.cool.core.io.readstore.MetaChunkRS;
import com.nus.cool.core.schema.TableSchema;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Map;

/**
 * @author hongbin
 * @version 0.1
 * @since 0.1
 */
public class IcebergLoader {

  private static CoolModel coolModel;

  public static List<BaseResult> executeQuery(CubeRS cube, IcebergQuery query) throws Exception {

    List<CubletRS> cublets = cube.getCublets();
    TableSchema tableSchema = cube.getSchema();
    List<BaseResult> results = new ArrayList<>();

    IcebergSelection selection = new IcebergSelection();
    selection.init(tableSchema, query);
    for (CubletRS cubletRS : cublets) {
      MetaChunkRS metaChunk = cubletRS.getMetaChunk();
      selection.process(metaChunk);
      if (selection.isbActivateCublet()) {
        List<ChunkRS> datachunks = cubletRS.getDataChunks();
        List<BitSet> bitSets = cubletRS.getBitSets();
        for (int i = 0; i < datachunks.size(); i++) {
          ChunkRS dataChunk = datachunks.get(i);
          BitSet bitSet;
          if (i >= bitSets.size()) {
            bitSet = new BitSet();
            bitSet.set(0, dataChunk.getRecords());
          } else {
            bitSet = bitSets.get(i);
          }
          if (bitSet.cardinality() == 0) {
            continue;
          }
          Map<String, BitSet> map = selection.process(dataChunk, bitSet);
          if (map == null) {
            continue;
          }
          for (Map.Entry<String, BitSet> entry : map.entrySet()) {
            String timeRange = entry.getKey();
            BitSet bs = entry.getValue();
            IcebergAggregation icebergAggregation = new IcebergAggregation();
            icebergAggregation.init(bs, query.getGroupFields(), metaChunk, dataChunk, timeRange);
            for (Aggregation aggregation : query.getAggregations()) {
              List<BaseResult> res = icebergAggregation.process(aggregation);
              results.addAll(res);
            }
          }
        }
      }
    }
    results = BaseResult.merge(results);
    return results;
  }

  public static QueryResult wrapResult(CubeRS cube, IcebergQuery query) {
    try {
      List<BaseResult> results = executeQuery(cube, query);
      return QueryResult.ok(results);
    } catch (Exception e) {
      e.printStackTrace();
    }
    return null;
  }

  public static void main(String[] args) throws IOException {

    coolModel = new CoolModel(args[0]);
    coolModel.reload(args[1]);

    ObjectMapper mapper = new ObjectMapper();

    IcebergQuery query = mapper.readValue(new File("/Users/zhiyi/Desktop/Cool/cool/query/iceberg-query.json"), IcebergQuery.class);

    QueryResult result = wrapResult(coolModel.getCube(query.getDataSource()), query);
    System.out.println(result.toString());
  }
}
