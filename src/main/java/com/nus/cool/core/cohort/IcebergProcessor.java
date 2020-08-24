package com.nus.cool.core.cohort;

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
import java.text.ParseException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Map;

public class IcebergProcessor {

  public static List<BaseResult> executeQuery(CubeRS cube, IcebergQuery query) throws ParseException {

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

}
