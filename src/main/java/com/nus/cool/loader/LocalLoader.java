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

import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;
import com.nus.cool.core.io.cache.CacheManager;
import com.nus.cool.core.io.writestore.ChunkWS;
import com.nus.cool.core.io.writestore.MetaChunkWS;
import com.nus.cool.core.schema.TableSchema;
import com.nus.cool.core.util.IntegerUtil;
import com.nus.cool.core.util.parser.CsvTupleParser;
import com.nus.cool.core.util.parser.TupleParser;
import com.nus.cool.core.util.reader.LineTupleReader;
import com.nus.cool.core.util.reader.TupleReader;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.*;
import java.util.List;

/**
 * @author hongbin, zhongle
 * @version 0.1
 * @since 0.1
 */
public class LocalLoader {

  private static int offset = 0;

  private static List<Integer> chunkOffsets = Lists.newArrayList();

  private static CacheManager cacheManager;

  private static String cubletFileName = null;

  public static void main(String[] args) throws IOException {
    File cubeRoot = new File(args[0]);
    TableSchema schema = TableSchema.read(new FileInputStream(new File(cubeRoot, "table.yaml")));
//        File dimensionFile = new File(cubeRoot, "dim_test.csv");
//        File dataFile = new File(cubeRoot, "test.csv");
    File dimensionFile = new File(cubeRoot, "dim_2.4G.csv");
    File dataFile = new File(cubeRoot, "data_2.4G.csv");
    File outputDir = new File(cubeRoot, args[1]);
    int chunkSize = 10000000;

    double memoryCacheSize = (double) 20 * 1024 * 1024 * 1024;
    double diskCacheSize = (double) 40 * 1024 * 1024 * 1024;

    cacheManager = new CacheManager(args[2], memoryCacheSize, diskCacheSize, 0.8);

    // TODO: Need to get from configuration
    boolean reuse = true;
    String storageLevel = "DISK_ONLY";

    load(schema, dimensionFile, dataFile, outputDir, chunkSize, reuse, storageLevel);
  }

  public static void load(TableSchema tableSchema, File dimensionFile, File dataFile,
      File outputDir, int chunkSize, boolean reuse, String storageLevel) throws IOException {
    TupleParser parser = new CsvTupleParser();
    MetaChunkWS metaChunk = newMetaChunk(dimensionFile, tableSchema, parser);
    DataOutputStream out = newCublet(outputDir, metaChunk);
    int userKeyIndex = tableSchema.getUserKeyField();
    String lastUser = null;
    try (TupleReader reader = new LineTupleReader(dataFile)) {
      int tuples = 0;
      ChunkWS chunk = ChunkWS
          .newChunk(tableSchema, metaChunk.getMetaFields(), offset, chunkOffsets.size() - 1,
              cubletFileName);
      while (reader.hasNext()) {
        String line = (String) reader.next();
        String[] tuple = parser.parse(line);
        String curUser = tuple[userKeyIndex];
        if (lastUser == null) {
          lastUser = curUser;
        }
        if (!curUser.equals(lastUser)) {
          lastUser = curUser;
          if (tuples >= chunkSize) {
            offset += chunk.writeTo(out, reuse, storageLevel, cacheManager);
            chunkOffsets.add(offset - Ints.BYTES);
            if (offset >= (1 << 30)) {
              closeCublet(out);
              out = newCublet(outputDir, metaChunk);
            }
            chunk = ChunkWS
                .newChunk(tableSchema, metaChunk.getMetaFields(), offset, chunkOffsets.size() - 1,
                    cubletFileName);
            tuples = 0;
          }
        }
        chunk.put(tuple);
        tuples++;
      }
      offset += chunk.writeTo(out, reuse, storageLevel, cacheManager);
      chunkOffsets.add(offset - Ints.BYTES);
      closeCublet(out);
    }
  }

  private static MetaChunkWS newMetaChunk(File inputMetaFile, TableSchema schema,
      TupleParser parser) throws IOException {
    MetaChunkWS metaChunk = MetaChunkWS.newMetaChunkWS(schema, offset);
    try (TupleReader reader = new LineTupleReader(inputMetaFile)) {
      while (reader.hasNext()) {
        metaChunk.put(parser.parse(reader.next()));
      }
    }
    metaChunk.complete();
    return metaChunk;
  }

  private static DataOutputStream newCublet(File dir, MetaChunkWS metaChunk) throws IOException {
    File cublet = new File(dir, Long.toHexString(System.currentTimeMillis()) + ".dz");
    String cubletFile = cublet.getName();
    cubletFileName = cubletFile.substring(0, cubletFile.length() - 3);
    DataOutputStream out = new DataOutputStream(new FileOutputStream(cublet));
    offset = metaChunk.writeTo(out);
    chunkOffsets.clear();
    chunkOffsets.add(offset - Ints.BYTES);
    return out;
  }

  private static void closeCublet(DataOutputStream out) throws IOException {
    int headOffset = offset;
    out.writeInt(IntegerUtil.toNativeByteOrder(chunkOffsets.size()));
    for (int chunkOff : chunkOffsets) {
      out.writeInt(IntegerUtil.toNativeByteOrder(chunkOff));
    }
    out.writeInt(IntegerUtil.toNativeByteOrder(headOffset));
    out.flush();
    out.close();
    cubletFileName = null;
  }
}
