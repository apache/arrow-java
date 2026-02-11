.. Licensed to the Apache Software Foundation (ASF) under one
.. or more contributor license agreements.  See the NOTICE file
.. distributed with this work for additional information
.. regarding copyright ownership.  The ASF licenses this file
.. to you under the Apache License, Version 2.0 (the
.. "License"); you may not use this file except in compliance
.. with the License.  You may obtain a copy of the License at

..   http://www.apache.org/licenses/LICENSE-2.0

.. Unless required by applicable law or agreed to in writing,
.. software distributed under the License is distributed on an
.. "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
.. KIND, either express or implied.  See the License for the
.. specific language governing permissions and limitations
.. under the License.

================
Arrow Flight SQL
================

Introduction
============

Arrow Flight SQL is a framework built on Arrow Flight, to solve the M:N problem of database servers and database
clients. Flight SQL implements a protocol that can be implemented by servers and clients, much like an Language Server
Protocol, solves the problem of M editors and N programming languages needed syntax highlighting and context aware
actions.

Implementing Flight SQL in a database server makes it discoverable by any client that implements the Flight SQL
protocol.

Concepts
========

Apache Arrow Flight SQL builds on the Flight protocol by adding higher-level concepts and using dynamic dispatching of
commands and requests through the `Action`, `FlightInfo`, `Get` and `Put` primitives of Flight. The concepts are
documented in the `Flight SQL Specification`_.

- **Metadata**
    - *Catalog*: A catalog is a collection of metadata that describes the structure and organization of the database. Varies by implementation. Typically groups a set of schemas.
    - *Schema*: Grouping of tables, i.e., a database schema. Varies by implementation.
    - *CrossReference*: Foreign keys from a source table that references columns in a target table
    - *ImportedKeys*: Foreign keys in a given table
    - *ExportedKeys*: References in a given table
    - *PrimaryKeys*: Table primary keys
    - *SqlInfo*: Information about the SQL capabilities of the data system
    - *Tables*: Definition of tables in a catalog, schema, and/or filtered by name
    - *TableTypes*: Types of tables. Varies by implementation
- **Statements**
    - *PreparedStatementQuery*: A prepared query, allows the data system to compile a plan with parameter place holders, avoiding repeated recompilation. Also offers protection against SQL injection, by containerizing user inputs from the query structure. Used for querying data.
    - *PreparedStatementUpdate*: A prepared update, allows the data system to compile a plan with parameter place holders, avoiding repreated recompilation. Used for insert data.
    - *StatementQuery*: A query to execute and to return data.
    - *StatementUpdate*: An update to execute and store data.
    - *StatementIngest*: Offers data in a `FlightStream` to be stored by the data system.
    - *StatementSubstraitPlan*: A query defined in the data system agnostic query serialization format `Substrait`_.
- **Session management**
    - *Session*: Starting from when a client connects, until it closes the connection. Allows to set certain options.
    - *Transaction*: A set of statements that work on and see a consistent state of the database, either all succeed of all fail.
    - *SavePoint*: A point within a transaction that can be individually rolled back.

Each of these concepts ties in closely to relational database. However, each database can interpret a concept in whatever
way it likes. As long as the concept are logical implemented it should not matter to the client. A catalog should be a
collection of schemas. A schema is a collection of tables, and a table can have a type.

Through the `SqlInfo`_ construct, a database can convey a lot of information about with it does and doesn't support.
Things like whether the server supports UNION, which functions are available, etc.

FlightSqlProducer
=================

This set of concepts is transferred/implemented by using **dynamic dispatching** on the Flight functions:

- `getSchema`: Getting the schema for a FlightDescriptor
- `getStream`: Getting the FlightStream of data for a FlightDescriptor
- `getFlightInfo`: Getting the FlightEndpoint and Ticket for a FlightDescriptor
- `acceptPut`: Getting new data
- `listActions`: Listing all actions
- `doAction`: Executing an action

The interface `FlightSqlProducer` implements this dynamic dispatching. It is a `FlightProducer`, the base interface for
the Arrow Flight protocol. All the Flight methods have a default implementation. The interface also defines the
functions needed to be a FlightSQLProducer, i.e., the functions to which the Flight function are dispatched.

Functions `getSchema` and `getFlightInfo` expect a a `FlightDescriptor` with a `Command` in the command field. Based on
the type of `Command`, the higher level function is invoked in this excerpt of `getFlightInfo`:

.. code-block::

        final Any command = FlightSqlUtils.parseOrThrow(descriptor.getCommand());

        if (command.is(CommandStatementQuery.class)) {
          return getFlightInfoStatement(
              FlightSqlUtils.unpackOrThrow(command, CommandStatementQuery.class), context, descriptor);
        } else if (command.is(CommandStatementSubstraitPlan.class)) {
          return getFlightInfoSubstraitPlan(
              FlightSqlUtils.unpackOrThrow(command, CommandStatementSubstraitPlan.class),
              context,
              descriptor);
        } else if (command.is(CommandPreparedStatementQuery.class)) {
          return getFlightInfoPreparedStatement(
              FlightSqlUtils.unpackOrThrow(command, CommandPreparedStatementQuery.class),
              context,
              descriptor);
        } else if (command.is(CommandGetCatalogs.class)) {
          return getFlightInfoCatalogs(
              FlightSqlUtils.unpackOrThrow(command, CommandGetCatalogs.class), context, descriptor);
        } else if (command.is(CommandGetDbSchemas.class)) {
          return getFlightInfoSchemas(
              FlightSqlUtils.unpackOrThrow(command, CommandGetDbSchemas.class), context, descriptor);
        } else if (command.is(CommandGetTables.class)) {
          return getFlightInfoTables(
              FlightSqlUtils.unpackOrThrow(command, CommandGetTables.class), context, descriptor);
        } else if (command.is(CommandGetTableTypes.class)) {
          return getFlightInfoTableTypes(
              FlightSqlUtils.unpackOrThrow(command, CommandGetTableTypes.class), context, descriptor);
        } else if (command.is(CommandGetSqlInfo.class)) {
          return getFlightInfoSqlInfo(
        ...

It parses the command stored in the `FlightDescriptor` message. Then it matches it against the known commands. If a
corresponding function is found, it is executed by the implementing class of `FlightSqlProducer`. Hence, control is
carried over to the Flight SQL protocol provider, the one a data system implements.

`FlightInfo` responses, are expected to have a `TicketStatementQuery` or a `Command` subclass as the `Ticket`.

Like the Flight protocol, a `FlightSqlProducer` is free to direct a client to other or even multiple endpoints on
different servers. This allows to answer SQL queries in a location transparent, distributed way.

The `getStream` function expects either a `TicketStatementQuery` for executing a query or a command to return the
requested metadata (Catalog, Schemas, Tables, etc). If the `Ticket` is constructed correctly in the `getFlightInfo`
function, i.e., it contains a `TicketStatementQuery`, the `getStreamStatement` of the `FlightSqlProducer` is invoked.
Here the data system should implement the query handling.

Accepting writes and executing actions works along the same lines. The following code snippet is the `acceptPut`
function:

.. code-block::

    default Runnable acceptPut(FlightProducer.CallContext context, FlightStream flightStream, FlightProducer.StreamListener<PutResult> ackStream) {
        Any command = FlightSqlUtils.parseOrThrow(flightStream.getDescriptor().getCommand());
        if (command.is(FlightSql.CommandStatementUpdate.class)) {
            return this.acceptPutStatement((FlightSql.CommandStatementUpdate)FlightSqlUtils.unpackOrThrow(command, FlightSql.CommandStatementUpdate.class), context, flightStream, ackStream);
        } else if (command.is(FlightSql.CommandStatementIngest.class)) {
            return this.acceptPutStatementBulkIngest((FlightSql.CommandStatementIngest)FlightSqlUtils.unpackOrThrow(command, FlightSql.CommandStatementIngest.class), context, flightStream, ackStream);
        } else if (command.is(FlightSql.CommandStatementSubstraitPlan.class)) {
            return this.acceptPutSubstraitPlan((FlightSql.CommandStatementSubstraitPlan)FlightSqlUtils.unpackOrThrow(command, FlightSql.CommandStatementSubstraitPlan.class), context, flightStream, ackStream);
        } else if (command.is(FlightSql.CommandPreparedStatementUpdate.class)) {
            return this.acceptPutPreparedStatementUpdate((FlightSql.CommandPreparedStatementUpdate)FlightSqlUtils.unpackOrThrow(command, FlightSql.CommandPreparedStatementUpdate.class), context, flightStream, ackStream);
        } else if (command.is(FlightSql.CommandPreparedStatementQuery.class)) {
            return this.acceptPutPreparedStatementQuery((FlightSql.CommandPreparedStatementQuery)FlightSqlUtils.unpackOrThrow(command, FlightSql.CommandPreparedStatementQuery.class), context, flightStream, ackStream);
        } else {
            throw CallStatus.INVALID_ARGUMENT.withDescription("The defined request is invalid.").toRuntimeException();
        }
    }

Again, we see the parsing of the command in the `FlightDescriptor` command field. If the command is not recognized, an
error is returned. Otherwise, the command is matched to the corresponding function for execution.


Arrow Flight SQL Example
========================

.. code-block::

    package name.jorisgillis.arrow.sql.server;

    import com.google.protobuf.Any;
    import com.google.protobuf.Message;

    import name.jorisgillis.arrow.sql.client.exception.FlightSQLExecutionException;

    import org.apache.arrow.datafusion.*;
    import org.apache.arrow.flight.*;
    import org.apache.arrow.flight.sql.BasicFlightSqlProducer;
    import org.apache.arrow.flight.sql.FlightSqlProducer;
    import org.apache.arrow.flight.sql.impl.FlightSql;
    import org.apache.arrow.memory.RootAllocator;
    import org.apache.arrow.vector.VarBinaryVector;
    import org.apache.arrow.vector.VarCharVector;
    import org.apache.arrow.vector.VectorSchemaRoot;
    import org.apache.arrow.vector.ViewVarCharVector;
    import org.apache.arrow.vector.compression.CompressionCodec;
    import org.apache.arrow.vector.compression.CompressionUtil;
    import org.apache.arrow.vector.dictionary.DictionaryProvider;
    import org.apache.arrow.vector.ipc.ArrowFileReader;
    import org.apache.arrow.vector.ipc.ArrowFileWriter;
    import org.apache.arrow.vector.ipc.ArrowReader;
    import org.apache.arrow.vector.ipc.message.IpcOption;
    import org.apache.arrow.vector.types.pojo.*;
    import org.slf4j.Logger;
    import org.slf4j.LoggerFactory;
    import org.springframework.beans.factory.annotation.Autowired;

    import java.io.*;
    import java.nio.channels.WritableByteChannel;
    import java.nio.charset.StandardCharsets;
    import java.nio.file.Files;
    import java.nio.file.Path;
    import java.util.HashMap;
    import java.util.List;
    import java.util.Map;
    import java.util.concurrent.CompletableFuture;
    import java.util.concurrent.ExecutionException;

    /**
     * A Flight SQL producer that maintains a database with a single catalog and tables as CSV files.
     */
    public class Producer extends BasicFlightSqlProducer {

        private static final Logger LOGGER = LoggerFactory.getLogger(Producer.class);
        private static final String CATALOG_NAME = "default";
        private final RootAllocator allocator;
        private final Location location;

        private final Map<String, Path> tables;
        private final Path dataDir;

        @Autowired
        public Producer(RootAllocator allocator, Location location, String pathOfDataDirectory) throws IOException {
            this.allocator = allocator;
            this.location = location;
            this.dataDir = Path.of(pathOfDataDirectory);

            this.tables = new HashMap<>();
            loadingDataDir();
        }

        /**
         * Loads all the table files in the data dir and adds them to the tables map.
         *
         * <p>Ensures that the data dir exists, and creates it if it doesn't.
         */
        private void loadingDataDir() throws IOException {
            if (!dataDir.toFile().exists()) {
                Files.createDirectory(dataDir);
            }

            Files.walk(dataDir)
                .filter(path -> path.toFile().getName().endsWith(".arrow"))
                .forEach(path -> {
                    String tableName = path
                        .toFile()
                        .getName()
                        .replace(".arrow", "");
                    tables.put(tableName, path);
                });
        }

        /**
         * This implementation assumes a single catalog: `default`.
         *
         * <p>This method is called by the FlightSQL client to get the list of catalogs. The catalogs
         * are an implementation specific detail. In other words, you can choose to use it or not.
         *
         * <p>Do note that if you don't implement the method, the client will get back an exception
         * (that is the default implementation in `NoOpFlightSqlProducer`). If a client does not handle
         * this correctly, it is possible it can not interact with the server.
         *
         * @param listener An interface for sending data back to the client.
         */
        @Override
        public void getStreamCatalogs(CallContext context, ServerStreamListener listener) {
            Schema catalogSchema = new Schema(
                List.of(
                    new Field("catalog_name", FieldType.notNullable(new ArrowType.Utf8View()), null)
                )
            );

            try (
                ViewVarCharVector catalogNames = new ViewVarCharVector("catalog_name", allocator);
                VectorSchemaRoot root = new VectorSchemaRoot(catalogSchema, List.of(catalogNames), 0)
            ) {
                listener.start(root);

                // One-row dataset
                catalogNames.setSafe(0, "default".getBytes());
                root.setRowCount(1);
                listener.putNext();

                listener.completed();
            } catch (Throwable e) {
                listener.error(e);
            }
        }

        @Override
        public FlightInfo getFlightInfoTables(
            FlightSql.CommandGetTables request,
            CallContext context,
            FlightDescriptor descriptor
        ) {
            return super.getFlightInfoTables(request, context, descriptor);
        }

        /**
         * Returning a list of all known tables (could be empty list).
         *
         * <p>We end up in this function through the `getSchema` function of `FlightProducer`.
         * `FlightSqlProducer` handles the delegation of different requests/commands to the correct
         * function.
         *
         * <p>The tables metadata is always streamed using a fixed Schema. The fields db_schema_name,
         * table_type and table_schema can be filled in by the implementation as it pleases.
         *
         * @param command The command contains filters.
         * @param context Per-call context.
         * @param listener An interface for sending data back to the client.
         */
        @Override
        public void getStreamTables(
            FlightSql.CommandGetTables command,
            CallContext context,
            ServerStreamListener listener
        ) {
            // Not totally, completely efficient, but streaming to the max!
            // Putting a single table in a VectorSchemaRoot and sending it to the client.
            // It would be better to batch the tables into batches of 100 for example.
            try (
                var root = VectorSchemaRoot.create(FlightSqlProducer.Schemas.GET_TABLES_SCHEMA, allocator)
            ) {
                root.allocateNew();
                VarCharVector catalogVector = (VarCharVector) root.getVector("catalog_name");
                VarCharVector dbSchemaNameVector = (VarCharVector) root.getVector("db_schema_name");
                VarCharVector tableNameVector = (VarCharVector) root.getVector("table_name");
                VarCharVector tableTypeVector = (VarCharVector) root.getVector("table_type");
                VarBinaryVector schemaVector = (VarBinaryVector) root.getVector("table_schema");

                listener.start(root);

                for (Map.Entry<String, Path> entry : this.tables.entrySet()) {
                    String tableName = entry.getKey();
                    Path tablePath = entry.getValue();

                    catalogVector.setSafe(
                        0,
                        CATALOG_NAME.getBytes(StandardCharsets.UTF_8)
                    );
                    byte[] schemaName = "%s-schema".formatted(tableName).getBytes(
                        StandardCharsets.UTF_8
                    );
                    dbSchemaNameVector.setSafe(0, schemaName);
                    tableNameVector.setSafe(
                        0,
                        tableName.getBytes(StandardCharsets.UTF_8)
                    );
                    tableTypeVector.setSafe(
                        0,
                        "ARROW".getBytes(StandardCharsets.UTF_8)
                    );

                    if (command.getIncludeSchema()) {
                        try (
                            var fileReader = new FileInputStream(
                                tablePath.toFile()
                            );
                            var reader = new ArrowFileReader(
                                fileReader.getChannel(),
                                allocator,
                                CompressionCodec.Factory.INSTANCE
                            )
                        ) {
                            schemaVector.set(
                                0,
                                reader
                                    .getVectorSchemaRoot()
                                    .getSchema()
                                    .serializeAsMessage()
                            );
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }

                    root.setRowCount(1);
                    listener.putNext();
                }

                listener.completed();
            }
        }

        /**
         * Get the schema of the query.
         *
         * <p>This function receives a query and creates a ticket (which contains the query again). It
         * should actually analyze the query and return the schema of the query. But for this demo
         * project I've opted to return an empty schema.
         *
         * <p>See function `getStream` for more information.
         */
        @Override
        public FlightInfo getFlightInfoStatement(
            FlightSql.CommandStatementQuery command,
            CallContext context,
            FlightDescriptor descriptor
        ) {
            // This should analyze the query.
            // The Java bindings for DataFusion are not so extensive, thus, we cannot get the schema of
            // the query without executing the whole query. So, far the time being, will return an empty
            // schema.

            Schema emptySchema = new Schema(List.of());

            // Putting the query into a new TicketStatementQuery
            FlightSql.TicketStatementQuery ticketStatementQuery =
                FlightSql.TicketStatementQuery.newBuilder()
                    .setStatementHandle(command.getQueryBytes())
                    .build();

            // IMPORTANT:
            // Ticket needs to be packed correctly!
            byte[] statementQuerySerialized = Any.pack(
                ticketStatementQuery
            ).toByteArray();
            Ticket ticket = new Ticket(statementQuerySerialized);

            return new FlightInfo(
                emptySchema,
                descriptor,
                List.of(FlightEndpoint.builder(ticket, location).build()),
                -1,
                -1
            );
        }

        /**
         * Executing a Query!
         *
         * <p>Executed whenever the `getStream` function is executed on the client. The client provides
         * us with a Ticket. In this implementation, the query is simply passed back to the client by
         * the `getFlightInfoStatement` function in the producer. When we get the ticket back, only
         * then, we execute the query using the DataFusion query engine.
         *
         * <p>Note that it is possible to invert that flow and execute the query in the
         * `getFlightInfoStatement` function and storing the result of the query temporarily on disk.
         * Then, in the `getStream` function, we can simply read the data from disk and send the data
         * back. A UUID could be used as the Ticket to retrieve the data.
         *
         * @param ticket Ticket message containing the statement handle.
         * @param context Per-call context.
         * @param listener An interface for sending data back to the client.
         */
        @Override
        public void getStreamStatement(
            FlightSql.TicketStatementQuery ticket,
            CallContext context,
            ServerStreamListener listener
        ) {
            try (var ctx = SessionContexts.create()) {
                // Registering all tables!
                for (Map.Entry<String, Path> entry : this.tables.entrySet()) {
                    ctx.registerTable(
                        entry.getKey(),
                        new ListingTable(
                            new ListingTableConfig.Builder(dataDir.toString())
                                .withListingOptions(
                                    ListingOptions.builder(
                                        new ArrowFormat()
                                    ).build()
                                )
                                .build(ctx)
                                .get()
                        )
                    );
                }

                LOGGER.info(
                    "Query = {}",
                    ticket.getStatementHandle().toStringUtf8()
                );
                CompletableFuture<DataFrame> result = ctx.sql(
                    ticket.getStatementHandle().toStringUtf8()
                );

                DataFrame dataFrame = result.get();
                ArrowReader reader = dataFrame.collect(allocator).get();

                listener.start(reader.getVectorSchemaRoot());
                while (reader.loadNextBatch()) {
                    listener.putNext();
                }
                listener.completed();
            } catch (ExecutionException e) {
                LOGGER.error("Execution Exception", e);
                throw new FlightSQLExecutionException(e);
            } catch (Exception e) {
                LOGGER.error("Error while executing query", e);
                listener.error(e);
            }
        }

        /**
         * Whenever a client calls the FlightSQL `executeIngest` function on the client, we end up here.
         */
        @Override
        public Runnable acceptPutStatementBulkIngest(
            FlightSql.CommandStatementIngest command,
            CallContext context,
            FlightStream flightStream,
            StreamListener<PutResult> ackStream
        ) {
            return () ->
                processIngestFlightStream(command, flightStream, ackStream);
        }

        private void processIngestFlightStream(
            FlightSql.CommandStatementIngest command,
            FlightStream flightStream,
            StreamListener<PutResult> ackStream
        ) {
            String tableName = command.getTable();
            LOGGER.info("Writing to table {}", tableName);

            while (flightStream.next()) {
                VectorSchemaRoot root = flightStream.getRoot();

                // Creating directory for table
                Path tablePath = dataDir.resolve("%s.arrow".formatted(tableName));
                if (!tablePath.toFile().exists()) {
                    try {
                        LOGGER.info(
                            "Creating table file for {} at {}",
                            command.getTable(),
                            tablePath
                        );
                        Files.createFile(tablePath);
                        tables.put(tableName, tablePath);
                    } catch (IOException e) {
                        LOGGER.error("Could not create directory", e);
                        ackStream.onError(e);
                    }
                }

                // Writing out file(s)
                try (
                    var fileOutputStream = new FileOutputStream(tablePath.toFile());
                    WritableByteChannel fileChannel = fileOutputStream.getChannel();
                    ArrowFileWriter arrowWriter = new ArrowFileWriter(
                        root,
                        new DictionaryProvider.MapDictionaryProvider(),
                        fileChannel,
                        Map.of(),
                        IpcOption.DEFAULT,
                        CompressionCodec.Factory.INSTANCE,
                        CompressionUtil.CodecType.ZSTD
                    )
                ) {
                    arrowWriter.start();
                    arrowWriter.writeBatch();
                    arrowWriter.end();
                } catch (Throwable e) {
                    LOGGER.error("Could not write out VectorSchemaRoot", e);
                    ackStream.onError(e);
                } finally {
                    ackStream.onNext(PutResult.empty());
                }
            }

            LOGGER.info("Ingestion completed");
            ackStream.onCompleted();
        }

        /**
         * Called whenever a `getFlightInfo...` function is not overridden.
         *
         * <p>This is the fallback option to convert a FlightDescriptor into a list of FlightEndpoints.
         * If you do not override, functions like `getFlightInfoCatalogs`, `getFlightInfoSchemas` and
         * `getFlightInfoTables` will default to this function to generate FlightEndpoints and Tickets.
         *
         * <p>This implementation essentially takes the command from the `FlightDescriptor` and puts it
         * in a Ticket. The companion functions `getStreamCatalogs`, `getStreamSchemas` and
         * `getStreamTables` will then be called to generate the actual data. They receive the command
         * passed to the `getFlightInfo...` function and do the work.
         */
        @Override
        protected <T extends Message> List<FlightEndpoint> determineEndpoints(
            T request,
            FlightDescriptor flightDescriptor,
            Schema schema
        ) {
            return List.of(
                FlightEndpoint.builder(
                    new Ticket(flightDescriptor.getCommand()),
                    location
                ).build()
            );
        }
    }


.. seealso::

   :external+arrow:doc:`Flight SQL protocol documentation <format/FlightSql>`
        Documentation of the Flight SQL protocol.

For usage information, see the `API documentation`_.

.. _API documentation: https://arrow.apache.org/java/current/reference/org.apache.arrow.flight.sql/module-summary.html
.. _Flight SQL Specification: https://arrow.apache.org/docs/format/FlightSql.html
.. _Substrait: https://substrait.io/
.. _SqlInfo: https://arrow.apache.org/java/current/reference/org.apache.arrow.flight.core/org/apache/arrow/flight/sql/impl/FlightSql.SqlInfo.html
