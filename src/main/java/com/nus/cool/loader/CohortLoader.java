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
import com.google.common.collect.Maps;
import com.nus.cool.core.cohort.CohortAggregation;
import com.nus.cool.core.cohort.CohortKey;
import com.nus.cool.core.cohort.CohortQuery;
import com.nus.cool.core.cohort.CohortSelection;
import com.nus.cool.core.cohort.FieldSet;
import com.nus.cool.core.cohort.QueryResult;
import com.nus.cool.core.io.cache.CacheManager;
import com.nus.cool.core.io.compression.SimpleBitSetCompressor;
import com.nus.cool.core.io.readstore.ChunkRS;
import com.nus.cool.core.io.readstore.CubeRS;
import com.nus.cool.core.io.readstore.CubletRS;
import com.nus.cool.core.io.readstore.MetaChunkRS;
import com.nus.cool.core.io.readstore.MetaFieldRS;
import com.nus.cool.core.schema.TableSchema;
import com.nus.cool.core.util.IntegerUtil;
import com.nus.cool.core.util.converter.DayIntConverter;
import com.nus.cool.core.util.converter.NumericConverter;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileFilter;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Map;

/**
 * @author hongbin, zhongle
 * @version 0.1
 * @since 0.1
 */
public class CohortLoader {

  public List<CohortQuery> load() throws IOException {
    List<CohortQuery> queries = Lists.newArrayList();

    CohortQuery query1 = new CohortQuery();
    query1.setDataSource("sogamo");
    query1.setAgeInterval(1);
    query1.setMetric("Retention");
    String[] cohortFields = {"country"};
    query1.setCohortFields(cohortFields);
    List<FieldSet> birthSelection = new ArrayList<>();
    List<String> values = new ArrayList<>();
    values.add("2013-05-20|2013-05-20");
    FieldSet fieldSet = new FieldSet(FieldSet.FieldSetType.Set, "eventDay", values);
    birthSelection.add(fieldSet);
    query1.setBirthSelection(birthSelection);
    query1.setBirthActions(new String[]{"launch"});
    query1.setAppKey("fd1ec667-75a4-415d-a250-8fbb71be7cab");

    CohortQuery query2 = new CohortQuery();
    query2.setDataSource("sogamo");
    query2.setAgeInterval(1);
    query2.setMetric("Retention");
    query2.setCohortFields(cohortFields);
    query2.setBirthSelection(birthSelection);
    query2.setBirthActions(new String[]{"launch", "fight"});
    query2.setAppKey("fd1ec667-75a4-415d-a250-8fbb71be7cab");

    queries.add(query1);
    queries.add(query2);
    return queries;
  }
}
