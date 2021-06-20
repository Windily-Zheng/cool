package com.nus.cool.core.cohort;

import com.nus.cool.core.iceberg.query.Aggregation;
import com.nus.cool.core.iceberg.query.IcebergAggregation;
import com.nus.cool.core.iceberg.query.IcebergQuery;
import com.nus.cool.core.iceberg.query.IcebergSelection;
import com.nus.cool.core.iceberg.result.BaseResult;
import com.nus.cool.core.io.cache.CacheManager;
import com.nus.cool.core.io.readstore.ChunkRS;
import com.nus.cool.core.io.readstore.CubeRS;
import com.nus.cool.core.io.readstore.CubletRS;
import com.nus.cool.core.io.readstore.MetaChunkRS;
import com.nus.cool.core.schema.TableSchema;
import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Map;

public class IcebergProcessor {

  // for testing
  public static double totalLoadTime = 0;

  // for testing
  public static double totalGenerateTime = 0;

  // for testing
  public static double totalFilterTime = 0;

  // for testing
  public static double totalSelectionTime = 0;

  // for testing
  public static double totalAggregationTime = 0;

  // for testing
  public static double totaltimeFilterTime = 0;

  public static List<BaseResult> executeQuery(CubeRS cube, IcebergQuery query,
      CacheManager cacheManager) throws IOException, ParseException {
    // TODO: Need to get from query
    boolean reuse = true;
    String storageLevel = "MEMORY_ONLY";
    System.out.println("reuse: " + reuse + "\n" + "storageLevel: " + storageLevel);

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
        String cubletFile = cubletRS.getFile();
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
          Map<String, BitSet> map = selection
              .process(dataChunk, bitSet, reuse, cacheManager, storageLevel,
                  cubletFile.substring(0, cubletFile.length() - 3));

          if (map == null) {
            continue;
          }
          long aggregationStart = System.nanoTime();
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
          long aggregationEnd = System.nanoTime();
          totalAggregationTime += (aggregationEnd - aggregationStart);
        }
      }
    }
    // for testing
    totalLoadTime += selection.totalLoadTime;
    totalGenerateTime += selection.totalGenerateTime;
    totalFilterTime += selection.totalFilterTime;
    totalSelectionTime += selection.totalSelectionTime;
    totaltimeFilterTime += selection.totalTimeFilterTime;

    results = BaseResult.merge(results);
    return results;
  }

}
