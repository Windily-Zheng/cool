package com.nus.cool.core.cohort;

import com.google.common.collect.Lists;
import com.nus.cool.core.io.cache.CacheManager;
import com.nus.cool.core.io.readstore.ChunkRS;
import com.nus.cool.core.io.readstore.CubeRS;
import com.nus.cool.core.io.readstore.CubletRS;
import com.nus.cool.core.io.readstore.MetaChunkRS;
import com.nus.cool.core.io.readstore.MetaFieldRS;
import com.nus.cool.core.schema.TableSchema;
import com.nus.cool.core.util.converter.DayIntConverter;
import com.nus.cool.core.util.converter.NumericConverter;
import com.nus.cool.loader.ResultTuple;
import java.io.IOException;
import java.util.BitSet;
import java.util.List;
import java.util.Map;

public class CohortProcessor {

  public List<ResultTuple> executeQuery(CubeRS cube, CohortQuery query, CacheManager cacheManager)
      throws IOException {
    List<CubletRS> cublets = cube.getCublets();
    TableSchema schema = cube.getSchema();
    List<ResultTuple> resultSet = Lists.newArrayList();
//    boolean tag = query.getOutSource() != null;
    List<BitSet> bitSets = Lists.newArrayList();
    for (CubletRS cublet : cublets) {
      MetaChunkRS metaChunk = cublet.getMetaChunk();
      CohortSelection sigma = new CohortSelection();
      CohortAggregation gamma = new CohortAggregation(sigma);
      gamma.init(schema, query);
      gamma.process(metaChunk);
      if (sigma.isBUserActiveCublet()) {
        List<ChunkRS> dataChunks = cublet.getDataChunks();
        for (ChunkRS dataChunk : dataChunks) {
          String cubletFile = cublet.getFile();

          // TODO: Need to get from query
          boolean reuse = true;
          String storageLevel = "DISK_ONLY";

          gamma.process(dataChunk, reuse, cacheManager, storageLevel,
              cubletFile.substring(0, cubletFile.length() - 3));
          bitSets.add(gamma.getBs());
        }
      }
//      if (tag) {
//        int end = cublet.getLimit();
//        DataOutputStream out = map.get(cublet.getFile());
//        for (BitSet bs : bitSets) {
//          SimpleBitSetCompressor.compress(bs, out);
//        }
//        out.writeInt(IntegerUtil.toNativeByteOrder(end));
//        out.writeInt(IntegerUtil.toNativeByteOrder(bitSets.size()));
//        out.writeInt(IntegerUtil.toNativeByteOrder(0));
//      }

      String cohortField = query.getCohortFields()[0];
      String actionTimeField = schema.getActionTimeFieldName();
      NumericConverter converter =
          cohortField.equals(actionTimeField) ? new DayIntConverter() : null;
      MetaFieldRS cohortMetaField = metaChunk.getMetaField(cohortField);
      Map<CohortKey, Long> results = gamma.getCubletResults();
      for (Map.Entry<CohortKey, Long> entry : results.entrySet()) {
        CohortKey key = entry.getKey();
        int cId = key.getCohort();
        String cohort = converter == null ? cohortMetaField.getString(key.getCohort())
            : converter.getString(cId);
        int age = key.getAge();
        long measure = entry.getValue();
        resultSet.add(new ResultTuple(cohort, age, measure));
      }
    }
    return ResultTuple.merge(resultSet);
  }

}
