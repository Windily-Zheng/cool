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
package com.nus.cool.core.io.writestore;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.primitives.Ints;
import com.nus.cool.core.io.Output;
import com.nus.cool.core.io.cache.CacheManager;
import com.nus.cool.core.io.compression.OutputCompressor;
import com.nus.cool.core.schema.ChunkType;
import com.nus.cool.core.schema.FieldSchema;
import com.nus.cool.core.schema.FieldType;
import com.nus.cool.core.schema.TableSchema;
import com.nus.cool.core.util.IntegerUtil;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

/**
 * DataChunk write store
 * <p>
 * DataChunk layout --------------------------------------------- | chunk data | chunk header |
 * header offset | ---------------------------------------------
 * <p>
 * chunk data layout ------------------------------------- | field 1 | field 2 | ... | field N |
 * -------------------------------------
 * <p>
 * chunk header layout -------------------------------------------------------------- | chunk type |
 * chunk ID | #records | #fields | field offsets | --------------------------------------------------------------
 * where ChunkType == ChunkType.DATA chunk ID == identifier of a chunk (unique in a cublet) #records
 * == number of records #fields == number of fields
 *
 * @author zhongle
 * @version 0.1
 * @since 0.1
 */
public class ChunkWS implements Output {

  /**
   * Chunk offset
   */
  private int offset;

  /**
   * Number of records
   */
  private int count;

  /**
   * Identifier of a chunk (unique in a cublet)
   */
  private int chunkID;

  /**
   * Name of a cublet file
   */
  private String cubletFileName;

  /**
   * Fields in data chunk
   */
  private FieldWS[] fields;

  /**
   * Create a chunk instance
   *
   * @param offset Offset in out stream
   * @param fields fields for this data chunk
   */
  public ChunkWS(int offset, FieldWS[] fields, int chunkID, String cubletFileName) {
    this.fields = checkNotNull(fields);
    checkArgument(offset > 0 && fields.length > 0);
    this.offset = offset;
    this.chunkID = chunkID;
    this.cubletFileName = checkNotNull(cubletFileName);
  }

  public static ChunkWS newChunk(TableSchema schema, MetaFieldWS[] metaFields, int offset,
      int chunkID, String cubletFileName) {
    OutputCompressor compressor = new OutputCompressor();
    List<FieldSchema> fieldSchemaList = schema.getFields();
    FieldWS[] fields = new FieldWS[fieldSchemaList.size()];
    int i = 0;
    for (FieldSchema fieldSchema : fieldSchemaList) {
      FieldType fieldType = fieldSchema.getFieldType();
      switch (fieldType) {
        case AppKey:
        case UserKey:
        case Action:
        case Segment:
          fields[i] = new HashFieldWS(fieldType, i, metaFields[i], compressor,
              fieldSchema.isPreCal(), cubletFileName, fieldSchema.getName(), chunkID);
          break;
        case ActionTime:
        case Metric:
          fields[i] = new RangeFieldWS(fieldType, i, compressor);
          break;
        default:
          throw new IllegalArgumentException("Unsupported FieldType: " + fieldType);
      }
      i++;
    }
    return new ChunkWS(offset, fields, chunkID, cubletFileName);
  }

  /**
   * Put a tuple into the chunk
   *
   * @param tuple plain data
   * @throws IOException If an I/O error occurs
   */
  public void put(String[] tuple) throws IOException {
    this.count++;
    for (int i = 0; i < tuple.length; i++) {
      this.fields[i].put(tuple);
    }
  }

  /**
   * Write ChunkWS to out stream and return bytes written
   *
   * @param out stream can write data to output stream
   * @return bytes written
   * @throws IOException If an I/O error occurs
   */
  @Override
  public int writeTo(DataOutput out) throws IOException {
    throw new UnsupportedOperationException();
  }

  /**
   * Write ChunkWS to out stream and return bytes written
   *
   * @param out stream can write data to output stream
   * @return bytes written
   * @throws IOException If an I/O error occurs
   */
  @Override
  public int writeTo(DataOutput out, boolean reuse, String storageLevel, CacheManager cacheManager)
      throws IOException {
    int bytesWritten = 0;
    int[] offsets = new int[this.fields.length];
    // Calculate offset of field
    // Write field
    for (int i = 0; i < this.fields.length; i++) {
      offsets[i] = this.offset + bytesWritten;
      if (this.fields[i] instanceof HashFieldWS) {
        bytesWritten += this.fields[i].writeTo(out, reuse, storageLevel, cacheManager);
        System.out.println(this.fields[i].getFieldType());
      } else {
        bytesWritten += this.fields[i].writeTo(out);
      }
    }

    // Calculate offset of header
    int chunkHeadOff = this.offset + bytesWritten;
    // Write chunkType (DATA)
    out.write(ChunkType.DATA.ordinal());
    bytesWritten++;
    // Write chunkID
    out.writeInt(IntegerUtil.toNativeByteOrder(this.chunkID));
    bytesWritten += Ints.BYTES;
    // Write #records
    out.writeInt(IntegerUtil.toNativeByteOrder(this.count));
    bytesWritten += Ints.BYTES;
    // Write #fields
    out.writeInt(IntegerUtil.toNativeByteOrder(this.fields.length));
    bytesWritten += Ints.BYTES;
    // Write field offsets
    for (int offset : offsets) {
      out.writeInt(IntegerUtil.toNativeByteOrder(offset));
      bytesWritten += Ints.BYTES;
    }

    // Write header offset
    out.writeInt(IntegerUtil.toNativeByteOrder(chunkHeadOff));
    bytesWritten += Ints.BYTES;
    return bytesWritten;
  }
}
