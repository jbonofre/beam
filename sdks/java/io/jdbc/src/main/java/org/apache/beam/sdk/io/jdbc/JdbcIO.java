/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.io.jdbc;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import javax.annotation.Nullable;
import javax.sql.DataSource;

import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * IO to read and write data on JDBC.
 * <p>
 * <h3>Reading from JDBC datasource</h3>
 * <p>
 * JdbcIO source returns a bounded collection of {@codeT} as a
 * {@code PCollection<T>}. T is the type returned by the provided {@link RowMapper}.
 * </p>
 * <p>
 * To configure the JDBC source, you have to provide a {@link DataSource}. The username and
 * password to connect to the database are optionals. The following example illustrates how to
 * configure a JDBC source:
 * </p>
 * <pre>
 *   {@code
 *
 * pipeline.apply(JdbcIO.read()
 *   .withDataSource(myDataSource)
 *
 *   }
 * </pre>
 * <h3>Writing to JDBC datasource</h3>
 * <p>
 * JDBC sink supports writing records into a database. It expects a
 * {@code PCollection<T>}, converts the {@code T} elements as SQL statement
 * and insert into the database. T is the type expected by the provided {@link ElementInserter}.
 * </p>
 * <p>
 * Like the source, to configure JDBC sink, you have to provide a datasource. For instance:
 * </p>
 * <pre>
 *   {@code
 *
 * pipeline
 *   .apply(...)
 *   .apply(JdbcIO.write().withDataSource(myDataSource)
 *
 *   }
 * </pre>
 */
public class JdbcIO {

  private static final Logger LOGGER = LoggerFactory.getLogger(JdbcIO.class);

  /**
   * Read data from a JDBC datasource.
   *
   * @return a {@link Read} {@link PTransform}.
   */
  public static Read<?> read() {
    return new Read(new Read.JdbcOptions(null, null, null, null), null);
  }

  /**
   * Write data to a JDBC datasource.
   *
   * @return a {@link Write} {@link PTransform}.
   */
  public static Write<?> write() {
    return new Write(new Write.JdbcWriter(null, null, null, null));
  }

  private JdbcIO() {
  }

  /**
   * An interface used by the JdbcIO Read for mapping rows of a ResultSet on a per-row basis.
   * Implementations of this interface perform the actual work of mapping each row to a result
   * object used in the {@link PCollection}.
   */
  public interface RowMapper<T> extends Serializable {

      T mapRow(ResultSet resultSet);

  }

  /**
   * A {@link PTransform} to read data from a JDBC datasource.
   */
  public static class Read<T> extends PTransform<PBegin, PCollection<T>> {

    public Read<T> withDataSource(DataSource dataSource) {
      return new Read<T>(options.withDataSource(dataSource), rowMapper);
    }

    public Read<T> withQuery(String query) {
      return new Read<T>(options.withQuery(query), rowMapper);
    }

    public <X> Read<X> withRowMapper(RowMapper<X> rowMapper) {
      return new Read<X>(options, rowMapper);
    }

    public Read<T> withUsername(String username) {
      return new Read<T>(options.withUsername(username), rowMapper);
    }

    public Read<T> withPassword(String password) {
      return new Read<T>(options.withPassword(password), rowMapper);
    }

    private final JdbcOptions options;
    private final RowMapper<T> rowMapper;

    private Read(JdbcOptions options, RowMapper<T> rowMapper) {
      this.options = options;
      this.rowMapper = rowMapper;
    }

    @Override
    public PCollection<T> apply(PBegin input) {
      PCollection<T> output = input.apply(Create.of(options))
          .apply(ParDo.of(new ReadFn<>(rowMapper)));

      return output;
    }

    @Override
    public void validate(PBegin input) {
      Preconditions.checkNotNull(rowMapper, "rowMapper");
      options.validate();
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);

      options.populateDisplayData(builder);
    }

    @VisibleForTesting
    static class JdbcOptions implements Serializable {

      private final DataSource dataSource;
      private final String query;
      @Nullable
      private final String username;
      @Nullable
      private final String password;

      private JdbcOptions(DataSource dataSource, String query,
                          @Nullable String username,
                          @Nullable String password) {
        this.dataSource = dataSource;
        this.query = query;
        this.username = username;
        this.password = password;
      }

      public JdbcOptions withDataSource(DataSource dataSource) {
        return new JdbcOptions(dataSource, query, username, password);
      }

      public JdbcOptions withQuery(String query) {
        return new JdbcOptions(dataSource, query, username, password);
      }

      public JdbcOptions withRowMapper(RowMapper rowMapper) {
        return new JdbcOptions(dataSource, query, username, password);
      }

      public JdbcOptions withUsername(String username) {
        return new JdbcOptions(dataSource, query, username, password);
      }

      public JdbcOptions withPassword(String password) {
        return new JdbcOptions(dataSource, query, username, password);
      }

      public void validate() {
        Preconditions.checkNotNull(dataSource, "dataSource");
        Preconditions.checkNotNull(query, "query");
      }

      public void populateDisplayData(DisplayData.Builder builder) {
        builder.add(DisplayData.item("dataSource", dataSource.getClass().getName()));
        builder.add(DisplayData.item("query", query));
        builder.addIfNotNull(DisplayData.item("username", username));
      }

      public DataSource getDataSource() {
        return dataSource;
      }

      public String getQuery() {
        return query;
      }

      @Nullable
      public String getUsername() {
        return username;
      }

      @Nullable
      public String getPassword() {
        return password;
      }
    }

    /**
     * A {@link DoFn} executing the SQL query to read from the database.
     */
    public static class ReadFn<T> extends DoFn<JdbcOptions, T> {

      private final RowMapper<T> rowMapper;

      private ReadFn(RowMapper<T> rowMapper) {
        this.rowMapper = rowMapper;
      }

      @ProcessElement
      public void processElement(ProcessContext context) throws Exception {
        JdbcOptions options = context.element();

        try (Connection connection = (options.getUsername() != null)
            ? options.getDataSource().getConnection(options.getUsername(), options.getPassword())
            : options.getDataSource().getConnection()) {

          try (PreparedStatement statement = connection.prepareStatement(options.getQuery())) {
            try (ResultSet resultSet = statement.executeQuery()) {
              while (resultSet.next()) {
                T record = rowMapper.mapRow(resultSet);
                if (record != null) {
                  context.output(record);
                }
              }
            }
          }
        }
      }
    }
  }

  /**
   * An interface used by the JdbcIO Write for mapping {@link PCollection} elements as a  rows of a
   * ResultSet on a per-row basis.
   * Implementations of this interface perform the actual work of mapping each row to a result
   * object used in the {@link PCollection}.
   */
  public interface ElementInserter<T> extends Serializable {

    PreparedStatement insert(T element, Connection connection);

  }

  /**
   * A {@link PTransform} to write to a JDBC datasource.
   */
  public static class Write<T> extends PTransform<PCollection<T>, PDone> {

    public Write<T> withDataSource(DataSource dataSource) {
      return new Write<>(writer.withDataSource(dataSource));
    }

    public Write<T> withUsername(String username) {
      return new Write<>(writer.withUsername(username));
    }

    public Write<T> withPassword(String password) {
      return new Write<>(writer.withPassword(password));
    }

    public <X> Write<X> withElementInserter(ElementInserter<X> elementInserter) {
      return new Write<>(writer.withElementInserter(elementInserter));
    }

    private final JdbcWriter writer;

    private Write(JdbcWriter writer) {
      this.writer = writer;
    }

    @Override
    public PDone apply(PCollection<T> input) {
      input.apply(ParDo.of(writer));
      return PDone.in(input.getPipeline());
    }

    @Override
    public void validate(PCollection<T> input) {
      writer.validate();
    }

    private static class JdbcWriter<T> extends DoFn<T, Void> {

      private final DataSource dataSource;
      private final String username;
      private final String password;
      private final ElementInserter<T> elementInserter;

      private Connection connection;

      public JdbcWriter(DataSource dataSource, String username, String password,
                        ElementInserter<T> elementInserter) {
        this.dataSource = dataSource;
        this.username = username;
        this.password = password;
        this.elementInserter = elementInserter;
      }

      public JdbcWriter<T> withDataSource(DataSource dataSource) {
        return new JdbcWriter<>(dataSource, username, password, elementInserter);
      }

      public JdbcWriter<T> withUsername(String username) {
        return new JdbcWriter<>(dataSource, username, password, elementInserter);
      }

      public JdbcWriter<T> withPassword(String password) {
        return new JdbcWriter<>(dataSource, username, password, elementInserter);
      }

      public JdbcWriter<T> withElementInserter(ElementInserter<T> elementInserter) {
        return new JdbcWriter<>(dataSource, username, password, elementInserter);
      }

      public void validate() {
        Preconditions.checkNotNull(dataSource, "dataSource");
        Preconditions.checkNotNull(elementInserter, "elementInserter");
      }

      @Setup
      public void connectToDatabase() throws Exception {
        if (username != null) {
          connection = dataSource.getConnection(username, password);
        } else {
          connection = dataSource.getConnection();
        }
        connection.setAutoCommit(false);
      }

      @ProcessElement
      public void processElement(ProcessContext context) {
        T record = context.element();
        try {
          PreparedStatement statement = elementInserter.insert(record, connection);
          if (statement != null) {
            try {
              statement.executeUpdate();
            } finally {
              statement.close();
            }
          }
        } catch (Exception e) {
          LOGGER.warn("Can't insert data into table", e);
        }
      }

      @FinishBundle
      public void finishBundle(Context context) throws Exception {
        connection.commit();
      }

      @Teardown
      public void closeConnection() throws Exception {
        if (connection != null) {
          connection.close();
        }
      }

    }

  }

}
