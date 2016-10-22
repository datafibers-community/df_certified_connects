/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 **/

package io.notch.connect.source.avro;

import static org.apache.avro.Schema.Type.NULL;
import static org.apache.avro.Schema.Type.RECORD;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Parser;
import org.apache.avro.Schema.Type;
import org.apache.avro.SchemaParseException;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JsonFileSourceTask extends SourceTask {
  private static final Logger log = LoggerFactory.getLogger(JsonFileSourceTask.class);
  public static final String FILENAME_FIELD = "filename";
  public static final String POSITION_FIELD = "position";

  private String topic;
  private String location;
  private String glob;
  private int interval;
  private boolean overwrite;
  private Schema dataSchema;

  private List<Path> processedPaths = new ArrayList<Path>();
  private List<Path> inProgressPaths = new ArrayList<Path>();

  private String filename;
  private InputStream stream;
  private BufferedReader reader = null;
  private char[] buffer = new char[1024];
  private int offset = 0;
  private Long streamOffset;


  @Override
  public String version() {
    return AppInfoParser.getVersion();
  }

  @Override
  public void start(Map<String, String> props) {
    topic = props.get(JsonFileSourceConnector.TOPIC_CONFIG);
    location = props.get(JsonFileSourceConnector.FILE_LOCATION_CONFIG);
    glob = props.get(JsonFileSourceConnector.FILE_LOCATION_CONFIG)
        .concat(props.get(JsonFileSourceConnector.FILE_GLOB_CONFIG));
    interval = Integer.parseInt(props.get(JsonFileSourceConnector.FILE_INTERVAL_CONFIG)) * 1000;
    overwrite = Boolean.valueOf(props.get(JsonFileSourceConnector.FILE_OVERWRITE_CONFIG));

    findMatch();

    // Get avro schema from registry and build proper schema POJO from it
    String schemaUri = props.get(JsonFileSourceConnector.SCHEMA_URI_CONFIG);
    String schemaSubject = props.get(JsonFileSourceConnector.SCHEMA_SUBJECT_CONFIG);
    String schemaVersion = props.get(JsonFileSourceConnector.SCHEMA_VERSION_CONFIG);
    String fullUrl =
        String.format("%s/subjects/%s/versions/%s", schemaUri, schemaSubject, schemaVersion);

    String schemaString = null;
    BufferedReader br = null;
    try {
      StringBuilder response = new StringBuilder();
      String line;
      br = new BufferedReader(new InputStreamReader(new URL(fullUrl).openStream()));
      while ((line = br.readLine()) != null) {
        response.append(line);
      }

      JsonNode responseJson = new ObjectMapper().readValue(response.toString(), JsonNode.class);
      schemaString = responseJson.get("schema").asText();
    } catch (Exception ex) {
      throw new ConnectException("Unable to retrieve schema from Schema Registry", ex);
    } finally {
      try {
        if (br != null)
          br.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    org.apache.avro.Schema avroSchema = null;
    try {
      avroSchema = new Parser().parse(schemaString);
    } catch (SchemaParseException ex) {
      throw new ConnectException(
          String.format("Unable to succesfully parse schema from: %s", schemaString), ex);
    }

    SchemaBuilder builder = null;
    // TODO: Add other avro schema types
    switch (avroSchema.getType()) {
      case RECORD: {
        builder = SchemaBuilder.struct();
        break;
      }
      case STRING: {
        builder = SchemaBuilder.string();
        break;
      }
      case INT: {
        builder = SchemaBuilder.int32();
        break;
      }
      case BOOLEAN: {
        builder = SchemaBuilder.bool();
        break;
      }
      default:
        builder = SchemaBuilder.string();
    }

    if (avroSchema.getFullName() != null)
      builder.name(avroSchema.getFullName());
    if (avroSchema.getDoc() != null)
      builder.doc(avroSchema.getDoc());

    if (RECORD.equals(avroSchema.getType()) && avroSchema.getFields() != null
        && !avroSchema.getFields().isEmpty()) {
      for (org.apache.avro.Schema.Field field : avroSchema.getFields()) {
        boolean hasDefault = field.defaultValue() != null;

        SchemaBuilder innerBuilder = getInnerBuilder(field);

        if (hasDefault)
          innerBuilder.optional();

        builder.field(field.name(), innerBuilder.build());
      }
    }

    dataSchema = builder.build();

  }

  @Override
  public List<SourceRecord> poll() throws InterruptedException {
    if (!inProgressPaths.isEmpty()) {
      try {
        Path currentPath = inProgressPaths.remove(0);
        processedPaths.add(currentPath);
        filename = currentPath.getFileName().toString();
        stream = new FileInputStream(currentPath.toFile());

        Map<String, Object> offset = context.offsetStorageReader()
            .offset(Collections.singletonMap(FILENAME_FIELD, filename));
        if (offset != null && !overwrite) {
          log.info("Found previous offset, will not process {}", filename);
          return null;
        } else
          streamOffset = 0L;

        reader = new BufferedReader(new InputStreamReader(stream));
        log.info("Opened {} for reading", filename);
      } catch (FileNotFoundException e) {
        throw new ConnectException(String.format("Unable to open file %", filename), e);
      }
    } else {
      log.warn("********* Waiting for file that meets the glob criteria! *********");
      synchronized (this) {
        this.wait(interval);
        findMatch();
      }
      return null;
    }

    ArrayList<SourceRecord> records = new ArrayList<SourceRecord>();
    StringBuilder fileContent = new StringBuilder();

    try {
      final BufferedReader readerCopy;
      synchronized (this) {
        readerCopy = reader;
      }
      if (readerCopy == null)
        return null;

      int nread = 0;
      while (readerCopy.ready()) {
        nread = readerCopy.read(buffer, offset, buffer.length - offset);
        log.trace("Read {} bytes from {}", nread, filename);

        if (nread > 0) {
          offset += nread;
          if (offset == buffer.length) {
            char[] newbuf = new char[buffer.length * 2];
            System.arraycopy(buffer, 0, newbuf, 0, buffer.length);
            buffer = newbuf;
          }

          String line;
          do {
            line = extractLine();
            if (line != null) {
              line = line.trim();
              log.trace("Read a line from {}", filename);
              fileContent.append(line);
            }
          } while (line != null);
        }
      }

      if (nread <= 0)
        synchronized (this) {
          this.wait(1000);
        }

    } catch (IOException e) {
      throw new ConnectException(String.format("Unable to read file %", filename), e);
    }

    if (fileContent.length() > 0) {
      JsonNode json = null;
      try {
        json = new ObjectMapper().readValue(fileContent.toString(), JsonNode.class);
      } catch (IOException ex) {
        throw new ConnectException(String.format("Unable to parse %s into a valid JSON", filename),
            ex);
      }

      Struct struct = new Struct(dataSchema);
      Iterator<Entry<String, JsonNode>> iterator = json.getFields();
      while (iterator.hasNext()) {
        Entry<String, JsonNode> entry = iterator.next();
        Object value = null;
        org.apache.kafka.connect.data.Field theField = dataSchema.field(entry.getKey());
        if (theField != null) {
          switch (theField.schema().type()) {
            case STRING: {
              value = entry.getValue().asText();
              break;
            }
            case INT32: {
              value = entry.getValue().asInt();
              break;
            }
            case BOOLEAN: {
              value = entry.getValue().asBoolean();
              break;
            }
            default:
              value = entry.getValue().asText();
          }
        }
        struct.put(entry.getKey(), value);

      }

      records.add(new SourceRecord(offsetKey(filename), offsetValue(streamOffset), topic,
          dataSchema, struct));
    }

    return records;

  }

  @Override
  public void stop() {
    log.trace("Stopping");
    synchronized (this) {
      try {
        if (stream != null && stream != System.in) {
          stream.close();
          log.trace("Closed input stream");
        }
      } catch (IOException e) {
        log.error("Failed to close JsonFileSourceTask stream: ", e);
      }
      this.notify();
    }
  }

  /**
   * Looks for files that meet the glob criteria. If any found they will be added to the list of
   * files to be processed
   */
  private void findMatch() {
    final PathMatcher globMatcher = FileSystems.getDefault().getPathMatcher("glob:".concat(glob));

    try {
      Files.walkFileTree(Paths.get(location), new SimpleFileVisitor<Path>() {
        @Override
        public FileVisitResult visitFile(Path path, BasicFileAttributes attributes)
            throws IOException {
          if (globMatcher.matches(path)) {
            if (!processedPaths.contains(path)) {
              inProgressPaths.add(path);
            }
          }
          return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult visitFileFailed(Path file, IOException e) throws IOException {
          return FileVisitResult.CONTINUE;
        }
      });
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private SchemaBuilder getInnerBuilder(Field field) {
    return getInnerBuilder(field, field.schema().getType());
  }

  private SchemaBuilder getInnerBuilder(Field field, Type type) {
    boolean hasDefault = field.defaultValue() != null;
    SchemaBuilder innerBuilder = null;
    switch (type) {
      case STRING: {
        innerBuilder = SchemaBuilder.string();
        if (hasDefault)
          innerBuilder.defaultValue(field.defaultValue().asText());
        break;
      }
      case INT: {
        innerBuilder = SchemaBuilder.int32();
        if (hasDefault)
          innerBuilder.defaultValue(field.defaultValue().asInt());
        break;
      }
      case BOOLEAN: {
        innerBuilder = SchemaBuilder.bool();
        if (hasDefault)
          innerBuilder.defaultValue(field.defaultValue().asBoolean());
        break;
      }
      case UNION: {
        for (org.apache.avro.Schema schema : field.schema().getTypes()) {
          if (!schema.getType().equals(NULL))
            return getInnerBuilder(field, schema.getType());
        }
      }
      default:
        throw new ConnectException(
            "Unable to build schema because there is no case for type " + field.schema().getType());
    }
    return innerBuilder;
  }

  private String extractLine() {
    int until = -1, newStart = -1;
    for (int i = 0; i < offset; i++) {
      if (buffer[i] == '\n') {
        until = i;
        newStart = i + 1;
        break;
      } else if (buffer[i] == '\r') {
        // We need to check for \r\n, so we must skip this if we can't check the next char
        if (i + 1 >= offset)
          return null;

        until = i;
        newStart = (buffer[i + 1] == '\n') ? i + 2 : i + 1;
        break;
      }
    }

    if (until != -1) {
      String result = new String(buffer, 0, until);
      System.arraycopy(buffer, newStart, buffer, 0, buffer.length - newStart);
      offset = offset - newStart;
      if (streamOffset != null)
        streamOffset += newStart;
      return result;
    } else {
      return null;
    }
  }

  private Map<String, String> offsetKey(String filename) {
    return Collections.singletonMap(FILENAME_FIELD, filename);
  }

  private Map<String, Long> offsetValue(Long pos) {
    return Collections.singletonMap(POSITION_FIELD, pos);
  }

}
