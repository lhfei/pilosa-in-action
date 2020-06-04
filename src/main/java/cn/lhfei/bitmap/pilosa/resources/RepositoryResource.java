/*
 * Copyright 2010-2011 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package cn.lhfei.bitmap.pilosa.resources;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.google.gson.Gson;
import com.pilosa.client.PilosaClient;
import com.pilosa.client.QueryResponse;
import com.pilosa.client.csv.FileRecordIterator;
import com.pilosa.client.orm.Field;
import com.pilosa.client.orm.Index;

/**
 * @version 0.1
 *
 * @author Hefei Li
 *
 * @created Apr 28, 2020
 */

@RestController
public class RepositoryResource {
  private static final String INDEX_NAME = "repository";

  private static Gson gson = new Gson();

  @RequestMapping(value = "/_row", method = {RequestMethod.GET})
  public String row(@RequestParam String field) throws IOException {
    Index index = client.readSchema().index(INDEX_NAME);

    Field language = index.field(field);
    Field stargazer = index.field("stargazer");

    index.intersect(stargazer.row(14), stargazer.row(19));


    QueryResponse response = client.query(language.row(10l));

    return gson.toJson(response.getResult().getRow());
  }

  @RequestMapping(value = "/query", method = {RequestMethod.GET})
  public String query(@RequestParam String field) throws IOException {
    Index index = client.readSchema().index(INDEX_NAME);

    Field language = index.field(field);
    Field stargazer = index.field("stargazer");

    index.intersect(stargazer.row(14), stargazer.row(19));


    QueryResponse response = client.query(language.row(10l));

    return gson.toJson(response.getResult().getRow());
  }

  @RequestMapping(value = "/{index}/file", method = {RequestMethod.GET})
  public String importCsv(@PathVariable String indexName) throws IOException {
    Index index = client.readSchema().index(indexName);
    Field language = index.field(indexName);

    String data = "1,10\n" + "5,20\n" + "3,41\n";
    InputStream stream = new ByteArrayInputStream(data.getBytes("UTF-8"));
    FileRecordIterator iterator = FileRecordIterator.fromStream(stream, language);

    client.importField(language, iterator);

    return "";
  }


  @Autowired
  private PilosaClient client;


}
