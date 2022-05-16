/*******************************************************************************
 * Copyright 2020-2022 Exactpro (Exactpro Systems Limited)
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
package com.exactpro.th2.keyvaluestorage

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.DriverTimeoutException
import com.datastax.oss.driver.api.core.connection.ConnectionInitException
import com.datastax.oss.driver.api.core.cql.AsyncResultSet
import com.datastax.oss.driver.api.core.cql.Row
import com.datastax.oss.driver.api.core.cql.SimpleStatement
import com.datastax.oss.driver.api.core.servererrors.QueryExecutionException
import com.datastax.oss.driver.api.core.servererrors.QueryValidationException
import com.datastax.oss.driver.api.core.type.reflect.GenericType
import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.ObjectMapper
import kotlinx.coroutines.delay
import kotlinx.coroutines.future.await
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import mu.KotlinLogging
import java.io.File
import java.io.IOException
import java.math.BigInteger
import java.net.InetSocketAddress
import java.util.*
import java.util.concurrent.CompletionStage
import java.util.regex.Pattern


class CassandraConnector(args: Array<String>) {
    private val host: String?
    private val dataCenter: String?
    private val username: String?
    private val password: String?
    private val keyspace: String?
    private val port: String?

    companion object {
        private val logger = KotlinLogging.logger {}
        private val CASSANDRA_PASS = "CASSANDRA_PASS"
        private val FOLDER_PATH = "/var/th2/config"
    }

    private suspend fun <T> databaseRequestRetry(request: () -> T): T? {
        var goodRequest = false
        var result: T? = null
        while (!goodRequest) {
            try {
                result = request.invoke()
                goodRequest = true
            } catch (e: DriverTimeoutException) {
                println("try to reconnect")
            } catch (e: ConnectionInitException) {
                println("try to reconnect")
            } finally {
                delay(DB_RETRY_DELAY)
            }
        }
        return result
    }

    private fun getKeyspaceName(keyspaceNameStoragePath: String): String? {
        return try {
            val objectMapper = ObjectMapper()
            val jsonMap = objectMapper.readValue(File(keyspaceNameStoragePath), HashMap::class.java)
            return jsonMap["keyspace"].toString()
        } catch (e: IOException) {
            logger.error(e) { e.message }
            null
        }
    }

    private fun getDBCredentials(jsonCredentialsPath: String): DBCredentials? {
        return try {
            val objectMapper = ObjectMapper()
            objectMapper.readValue(File(jsonCredentialsPath), DBCredentials::class.java)
        } catch (e: IOException) {
            logger.error(e) { e.message }
            null
        }
    }

    private val UUID_REGEX_PATTERN = Pattern.compile("^[{]?[0-9a-fA-F]{8}-([0-9a-fA-F]{4}-){3}[0-9a-fA-F]{12}[}]?$")
    fun isValidUUID(str: String?): Boolean {
        return if (str == null) {
            false
        } else UUID_REGEX_PATTERN.matcher(str).matches()
    }

    var session: CqlSession? = null
    fun connect(callback: suspend () -> Unit) {
        session = CqlSession.builder()
            .addContactPoint(
                InetSocketAddress(host, port!!.toInt())
            )
            .withLocalDatacenter(dataCenter!!)
            .withAuthCredentials(username!!, password!!)
            .build()
        runBlocking {
            launch {
                callback.invoke()
            }
        }
    }

    suspend fun createPartitionKeyStorageIfNotExists() {
        try {
            val statement = SimpleStatement.newInstance(
                "CREATE TABLE IF NOT EXISTS $keyspace.$USER_PARTITION_KEY_STORAGE (userId UUID, partitionKey UUID, PRIMARY KEY(userId))"
            ).setTracing(true)
            val asyncResultSetCompletionStage: CompletionStage<AsyncResultSet>? = databaseRequestRetry {
                session!!.executeAsync(statement)
            }
            val resultSet = asyncResultSetCompletionStage?.await()
            logger.debug { "${resultSet?.executionInfo?.queryTrace}\nTIME: ${resultSet?.executionInfo?.queryTrace?.durationMicros}" }
        } catch (e: DriverTimeoutException) {
            logger.error(e) { e.message }
        } catch (e: QueryExecutionException) {
            logger.error(e) { e.message }
        } catch (e: QueryValidationException) {
            logger.error(e) { e.message }
        }
    }

    suspend fun getPartitionKeyByUserId(userId: String): UUID? {
        try {
            val asyncResultSetCompletionStage: CompletionStage<AsyncResultSet>? = databaseRequestRetry {
                val statement = SimpleStatement.newInstance(
                    "SELECT partitionKey FROM $keyspace.$USER_PARTITION_KEY_STORAGE WHERE userId = $userId;"
                ).setTracing(true)
                session!!.executeAsync(statement)
            }
            val resultSet = asyncResultSetCompletionStage?.await()
            val rows: MutableList<Row> = mutableListOf()
            resultSet?.currentPage()?.forEach { r -> rows.add(r) }

            logger.debug { "${resultSet?.executionInfo?.queryTrace}\nTIME: ${resultSet?.executionInfo?.queryTrace?.durationMicros}" }

            return rows[0].get("partitionKey", GenericType.UUID)
        } catch (e: DriverTimeoutException) {
            logger.error(e) { e.message }
            return null
        } catch (e: QueryExecutionException) {
            logger.error(e) { e.message }
            return null
        } catch (e: QueryValidationException) {
            logger.error(e) { e.message }
            return null
        }
    }

    suspend fun checkUserIdExists(userId: String): Boolean {
        try {
            val asyncResultSetCompletionStage: CompletionStage<AsyncResultSet>? = databaseRequestRetry {
                val statement = SimpleStatement.newInstance(
                    "SELECT * FROM $keyspace.$USER WHERE userId = $userId;"
                ).setTracing(true)
                session!!.executeAsync(statement)
            }
            val resultSet = asyncResultSetCompletionStage?.await()
            val rows: MutableList<Row> = mutableListOf()
            resultSet?.currentPage()?.forEach { r -> rows.add(r) }

            logger.debug { "${resultSet?.executionInfo?.queryTrace}\nTIME: ${resultSet?.executionInfo?.queryTrace?.durationMicros}" }

            return rows.isNotEmpty()
        } catch (e: DriverTimeoutException) {
            logger.error(e) { e.message }
            return false
        } catch (e: QueryExecutionException) {
            logger.error(e) { e.message }
            return false
        } catch (e: QueryValidationException) {
            logger.error(e) { e.message }
            return false
        }
    }

    suspend fun registerUser(name: String, preferences: String? = null): String? {
        try {
            val userId = UUID.randomUUID()
            val partitionKey = UUID.randomUUID()
            val userInsertionQuery = SimpleStatement.newInstance(
                "INSERT INTO $keyspace.$USER (userId, name, preferences) VALUES ($userId, '$name', '${preferences ?: "${DEFAULT_USER_PREFERENCES}" }');"
            ).setTracing(true)
            val userInsertionCompletionStage: CompletionStage<AsyncResultSet>? = databaseRequestRetry {
                session!!.executeAsync(userInsertionQuery)
            }

            val userInsertionResultSet = userInsertionCompletionStage?.await()
            logger.debug { "${userInsertionResultSet?.executionInfo?.queryTrace}\nTIME: ${userInsertionResultSet?.executionInfo?.queryTrace?.durationMicros}" }

            val partitionKeyInsertionQuery = SimpleStatement.newInstance(
                "INSERT INTO $keyspace.$USER_PARTITION_KEY_STORAGE (userId, partitionKey) VALUES ($userId, $partitionKey);"
            ).setTracing(true)
            val partitionKeyInsertionCompletionStage: CompletionStage<AsyncResultSet>? = databaseRequestRetry {
                session!!.executeAsync(partitionKeyInsertionQuery)
            }

            val partitionKeyInsertionResultSet = partitionKeyInsertionCompletionStage?.await()
            logger.debug { "${partitionKeyInsertionResultSet?.executionInfo?.queryTrace}\nTIME: ${partitionKeyInsertionResultSet?.executionInfo?.queryTrace?.durationMicros}" }

            return userId.toString()
        } catch (e: DriverTimeoutException) {
            logger.error(e) { e.message }
            return null
        } catch (e: QueryExecutionException) {
            logger.error(e) { e.message }
            return null
        } catch (e: QueryValidationException) {
            logger.error(e) { e.message }
            return null
        }
    }

    suspend fun getUserPreferences(userId:String): String? {
        try {
            val userInsertionQuery = SimpleStatement.newInstance(
                "SELECT preferences FROM $keyspace.$USER WHERE userId = ${UUID.fromString(userId)}"
            ).setTracing(true)

            val asyncResultSetCompletionStage: CompletionStage<AsyncResultSet>? = databaseRequestRetry {
                session!!.executeAsync(userInsertionQuery)
            }

            val resultSet = asyncResultSetCompletionStage?.await()

            logger.debug { "${resultSet?.executionInfo?.queryTrace}\nTIME: ${resultSet?.executionInfo?.queryTrace?.durationMicros}" }

            return resultSet?.one()?.getObject("preferences")?.toString()

        } catch (e: DriverTimeoutException) {
            logger.error(e) { e.message }
            return null
        } catch (e: QueryExecutionException) {
            logger.error(e) { e.message }
            return null
        } catch (e: QueryValidationException) {
            logger.error(e) { e.message }
            return null
        }
    }

    suspend fun updateUserPreferences(userId:String, preferences: String): String? {
        try {
            val userInsertionQuery = SimpleStatement.newInstance(
                "UPDATE $keyspace.$USER SET preferences = '$preferences' WHERE userId = ${UUID.fromString(userId)}"
            ).setTracing(true)

            val asyncResultSetCompletionStage: CompletionStage<AsyncResultSet>? = databaseRequestRetry {
                session!!.executeAsync(userInsertionQuery)
            }

            val resultSet = asyncResultSetCompletionStage?.await()

            logger.debug { "${resultSet?.executionInfo?.queryTrace}\nTIME: ${resultSet?.executionInfo?.queryTrace?.durationMicros}" }

            return resultSet?.one()?.getObject("preferences")?.toString()

        } catch (e: DriverTimeoutException) {
            logger.error(e) { e.message }
            return null
        } catch (e: QueryExecutionException) {
            logger.error(e) { e.message }
            return null
        } catch (e: QueryValidationException) {
            logger.error(e) { e.message }
            return null
        }
    }

    suspend fun checkUserNameExists(name: String): Boolean {
        try {
            val statement = SimpleStatement.newInstance(
                "SELECT * FROM $keyspace.$USER WHERE name = '$name' ALLOW FILTERING;"
            ).setTracing(true)
            val asyncResultSetCompletionStage: CompletionStage<AsyncResultSet>? = databaseRequestRetry {
                session!!.executeAsync(statement)
            }
            val resultSet = asyncResultSetCompletionStage?.await()
            val rows: MutableList<Row> = mutableListOf()
            resultSet?.currentPage()?.forEach { r -> rows.add(r) }

            logger.debug { "${resultSet?.executionInfo?.queryTrace}\nTIME: ${resultSet?.executionInfo?.queryTrace?.durationMicros}" }

            return rows.isNotEmpty()
        } catch (e: DriverTimeoutException) {
            logger.error(e) { e.message }
            return false
        } catch (e: QueryExecutionException) {
            logger.error(e) { e.message }
            return false
        } catch (e: QueryValidationException) {
            logger.error(e) { e.message }
            return false
        }
    }

    suspend fun createUserTableIfNotExists() {
        try {
            val statement = SimpleStatement.newInstance(
                "CREATE TABLE IF NOT EXISTS $keyspace.$USER (userId uuid, " +
                        "name text, preferences text, " +
                        "PRIMARY KEY (userId))"
            ).setTracing(true)
            val asyncResultSetCompletionStage: CompletionStage<AsyncResultSet>? = databaseRequestRetry {
                session!!.executeAsync(statement)
            }

            val resultSet = asyncResultSetCompletionStage?.await()
            logger.debug { "${resultSet?.executionInfo?.queryTrace}\nTIME: ${resultSet?.executionInfo?.queryTrace?.durationMicros}" }

        } catch (e: DriverTimeoutException) {
            logger.error(e) { e.message }
        } catch (e: QueryExecutionException) {
            logger.error(e) { e.message }
        } catch (e: QueryValidationException) {
            logger.error(e) { e.message }
        }
    }

    suspend fun createKeySpaceIfNotExists() {
        try {
            val statement = SimpleStatement.newInstance(
                "CREATE KEYSPACE IF NOT EXISTS $keyspace WITH replication = {'class':'SimpleStrategy', 'replication_factor' : 1}"
            ).setTracing(true)
            val asyncResultSetCompletionStage: CompletionStage<AsyncResultSet>? = databaseRequestRetry {
                session!!.executeAsync(statement)
            }

            val resultSet = asyncResultSetCompletionStage?.await()
            logger.debug { "${resultSet?.executionInfo?.queryTrace}\nTIME: ${resultSet?.executionInfo?.queryTrace?.durationMicros}" }

        } catch (e: DriverTimeoutException) {
            logger.error(e) { e.message }
        } catch (e: QueryExecutionException) {
            logger.error(e) { e.message }
        } catch (e: QueryValidationException) {
            logger.error(e) { e.message }
        }
    }

    suspend fun selectAllFromCollection(table: String?): List<String> {
        return try {
            val asyncResultSetCompletionStage: CompletionStage<AsyncResultSet>? = databaseRequestRetry {
                val statement = SimpleStatement.newInstance(
                    "SELECT * FROM $keyspace.$table;"
                ).setTracing(true)
                session!!.executeAsync(statement)
            }
            val resultSet = asyncResultSetCompletionStage?.await()
            val rows: MutableList<Row> = mutableListOf()
            resultSet?.currentPage()?.forEach { r -> rows.add(r) }
            val result: MutableList<String> = ArrayList()
            for (row in rows) {
                result.add(row.formattedContents)
            }
            logger.debug { "${resultSet?.executionInfo?.queryTrace}\nTIME: ${resultSet?.executionInfo?.queryTrace?.durationMicros}" }
            result
        } catch (e: DriverTimeoutException) {
            logger.error(e) { e.message }
            emptyList()
        } catch (e: QueryExecutionException) {
            logger.error(e) { e.message }
            emptyList()
        } catch (e: QueryValidationException) {
            logger.error(e) { e.message }
            emptyList()
        } catch (e: JsonProcessingException) {
            logger.error(e) { e.message }
            emptyList()
        }
    }

    suspend fun selectRecordsFromCollection(partitionKey: UUID, table: String?): List<String> {
        return try {
            val asyncResultSetCompletionStage: CompletionStage<AsyncResultSet>? = databaseRequestRetry {
                val statement = SimpleStatement.newInstance(
                    "SELECT * FROM $keyspace.$table WHERE partitionKey = $partitionKey;"
                ).setTracing(true)
                session!!.executeAsync(statement)
            }
            val resultSet = asyncResultSetCompletionStage?.await()
            logger.debug { "${resultSet?.executionInfo?.queryTrace?.parameters}\nTIME: ${resultSet?.executionInfo?.queryTrace?.durationMicros}" }
            val rows: MutableList<Row> = mutableListOf()
            resultSet?.currentPage()?.forEach { r -> rows.add(r) }
            val result: MutableList<String> = ArrayList()
            val records = sortByTimestamp(rows)
            for (record in records) {
                val mapper = ObjectMapper()
                result.add(mapper.writeValueAsString(record))
            }
            result
        } catch (e: DriverTimeoutException) {
            logger.error(e) { e.message }
            emptyList()
        } catch (e: QueryExecutionException) {
            logger.error(e) { e.message }
            emptyList()
        } catch (e: QueryValidationException) {
            logger.error(e) { e.message }
            emptyList()
        } catch (e: JsonProcessingException) {
            logger.error(e) { e.message }
            emptyList()
        }
    }

    suspend fun getCorrectId(partitionKey: UUID, collection: String?, id: String?): String? {
        return if (isValidUUID(id)) id.toString() else getUUIDByKeyName(partitionKey, collection, id)
    }

    private suspend fun createTableIfNotExists(table: String) {
        try {
            val statement = SimpleStatement.newInstance(
                "CREATE TABLE IF NOT EXISTS $keyspace.$table (partitionKey uuid, id uuid, " +
                        "json text, time bigint, " +
                        "PRIMARY KEY (partitionKey, id))"
            ).setTracing(true)
            val asyncResultSetCompletionStage: CompletionStage<AsyncResultSet>? = databaseRequestRetry {
                session!!.executeAsync(statement)
            }

            val resultSet = asyncResultSetCompletionStage?.await()
            logger.debug { "${resultSet?.executionInfo?.queryTrace}\nTIME: ${resultSet?.executionInfo?.queryTrace?.durationMicros}" }

        } catch (e: DriverTimeoutException) {
            logger.error(e) { e.message }
        } catch (e: QueryExecutionException) {
            logger.error(e) { e.message }
        } catch (e: QueryValidationException) {
            logger.error(e) { e.message }
        }
    }

    suspend fun setDefaultKey(collection: String, keyName: String, uuidValue: String) {
        try {
            val asyncResultSetCompletionStage = databaseRequestRetry {
                val statement =
                    SimpleStatement.newInstance(
                        "INSERT INTO $keyspace.$ALTERNATE_KEYS_STORAGE (name, collection, uuid_key) " +
                                "VALUES ('$keyName', '$collection', $uuidValue)"
                    ).setTracing(true)
                session!!.executeAsync(statement)
            }
            val resultSet = asyncResultSetCompletionStage?.await()
            logger.debug { "${resultSet?.executionInfo?.queryTrace?.parameters}\nTIME: ${resultSet?.executionInfo?.queryTrace?.durationMicros}" }
        } catch (e: DriverTimeoutException) {
            logger.error(e) { e.message }
        } catch (e: QueryExecutionException) {
            logger.error(e) { e.message }
        } catch (e: QueryValidationException) {
            logger.error(e) { e.message }
        }
    }

    suspend fun isAlternateKeyExists(partitionKey: UUID, collection: String, keyName: String): Boolean {
        return getUUIDByKeyName(partitionKey, collection, keyName) != null
    }

    private suspend fun getUUIDByKeyName(partitionKey: UUID, collection: String?, keyName: String?): String? {
        return try {
            val asyncResultSetCompletionStage = databaseRequestRetry {
                val statement =
                    SimpleStatement.newInstance(
                        "SELECT * FROM $keyspace.$ALTERNATE_KEYS_STORAGE WHERE partitionKey = $partitionKey name = '$keyName' AND collection = '$collection'"
                    ).setTracing(true)
                session!!.executeAsync(statement)
            }
            val resultSet = asyncResultSetCompletionStage?.await()
            logger.debug { "${resultSet?.executionInfo?.queryTrace?.parameters}\nTIME: ${resultSet?.executionInfo?.queryTrace?.durationMicros}" }
            resultSet?.one()?.getObject("uuid_key")?.toString().toString()
        } catch (e: DriverTimeoutException) {
            logger.error(e) { e.message }
            null
        } catch (e: QueryExecutionException) {
            logger.error(e) { e.message }
            null
        } catch (e: QueryValidationException) {
            logger.error(e) { e.message }
            null
        }
    }

    suspend fun createKeysTableIfNotExists() {
        try {
            databaseRequestRetry {
                session!!.executeAsync(
                    "CREATE TABLE IF NOT EXISTS $keyspace.$ALTERNATE_KEYS_STORAGE (name text, collection text, " +
                            "uuid_key uuid, " +
                            "PRIMARY KEY (name, collection))"
                )
            }
        } catch (e: DriverTimeoutException) {
            logger.error(e) { e.message }
        } catch (e: QueryExecutionException) {
            logger.error(e) { e.message }
        } catch (e: QueryValidationException) {
            logger.error(e) { e.message }
        }
    }

    suspend fun getByIdAndTimestampFromCollection(
        partitionKey: UUID,
        collection: String?,
        id: String?,
        timestamp: String?
    ): String? {
        return try {
            val asyncResultSetCompletionStage = databaseRequestRetry {
                val statement =
                    SimpleStatement.newInstance(
                        "SELECT * $keyspace.$collection WHERE partitionKey = $partitionKey AND id = $id AND time = $timestamp ALLOW FILTERING"
                    ).setTracing(true)
                session!!.executeAsync(statement)
            }
            val resultSet = asyncResultSetCompletionStage?.await()
            logger.debug { "${resultSet?.executionInfo?.queryTrace?.parameters}\nTIME: ${resultSet?.executionInfo?.queryTrace?.durationMicros}" }
            resultSet?.one()?.getObject("json")?.toString()
        } catch (e: DriverTimeoutException) {
            logger.error(e) { e.message }
            null
        } catch (e: QueryExecutionException) {
            logger.error(e) { e.message }
            null
        } catch (e: QueryValidationException) {
            logger.error(e) { e.message }
            null
        }
    }

    private fun sortByTimestamp(records: List<Row>): List<Record> {
        val comparableRecords: MutableList<Record> = ArrayList()
        for (row in records) {
            val record = Record(
                row.getObject("id").toString(),
                row.getObject("json").toString(), BigInteger(row.getObject("time").toString())
            )
            comparableRecords.add(record)
        }
        comparableRecords.sortWith(Record.Comparators.TIME)
        return comparableRecords
    }

    suspend fun getByIdFromCollection(partitionKey: UUID, collection: String?, id: String?): String? {
        return try {
            val asyncResultSetCompletionStage =
                databaseRequestRetry {
                    val statement =
                        SimpleStatement.newInstance("SELECT * FROM $keyspace.$collection WHERE partitionKey = $partitionKey AND id = $id")
                            .setTracing(true)
                    session!!.executeAsync(statement)
                }
            val resultSet = asyncResultSetCompletionStage?.await()
            logger.debug {
                "${resultSet?.executionInfo?.queryTrace?.parameters}\nTIME: " +
                        "${resultSet?.executionInfo?.queryTrace?.durationMicros}\n"
            }
            resultSet?.one()?.getObject("json")?.toString()
        } catch (e: DriverTimeoutException) {
            logger.error(e) { e.message }
            null
        } catch (e: QueryExecutionException) {
            logger.error(e) { e.message }
            null
        } catch (e: QueryValidationException) {
            logger.error(e) { e.message }
            null
        }
    }

    suspend fun getIdsFromCollection(table: String?): List<String> {
        return try {
            val idsList: MutableList<String> = ArrayList()
            val asyncResultSetCompletionStage = databaseRequestRetry {
                val statement =
                    SimpleStatement.newInstance("SELECT * FROM $keyspace.$table")
                        .setTracing(true)
                session!!.executeAsync(statement)
            }
            val resultSet = asyncResultSetCompletionStage?.await()
            val rows: MutableList<Row> = mutableListOf()
            logger.debug { "${resultSet?.executionInfo?.queryTrace?.parameters}\nTIME: ${resultSet?.executionInfo?.queryTrace?.durationMicros}\n" }
            resultSet?.currentPage()?.forEach { row -> rows.add(row) }
            val records = sortByTimestamp(rows)
            for (record in records) {
                record.id.let { idsList.add(it) }
            }
            idsList
        } catch (e: DriverTimeoutException) {
            logger.error(e) { e.message }
            emptyList()
        } catch (e: QueryExecutionException) {
            logger.error(e) { e.message }
            emptyList()
        } catch (e: QueryValidationException) {
            logger.error(e) { e.message }
            emptyList()
        }
    }

    private suspend fun allTables(): List<String> {
        return try {
            val tables: MutableList<String> = ArrayList()
            val asyncResultSetCompletionStage = databaseRequestRetry {
                val statement =
                    SimpleStatement.newInstance("SELECT * FROM system_schema.tables WHERE keyspace_name = '$keyspace'")
                        .setTracing(true)
                session!!.executeAsync(statement)
            }
            val rows: MutableList<Row> = mutableListOf()
            val resultSet = asyncResultSetCompletionStage?.await()
            logger.debug { "${resultSet?.executionInfo?.queryTrace?.parameters}\nTIME: ${resultSet?.executionInfo?.queryTrace?.durationMicros}\n" }
            resultSet?.currentPage()?.forEach { row -> rows.add(row) }
            for (row in rows) {
                tables.add(Objects.requireNonNull(row.getObject("table_name")).toString())
            }
            tables
        } catch (e: DriverTimeoutException) {
            logger.error(e) { e.message }
            emptyList()
        } catch (e: QueryExecutionException) {
            logger.error(e) { e.message }
            emptyList()
        } catch (e: QueryValidationException) {
            logger.error(e) { e.message }
            emptyList()
        }
    }

    suspend fun isCollectionExists(table: String?): Boolean = allTables().contains(table)

    suspend fun insertIntoTable(partitionKey: UUID, table: String, row: String): String? {
        val uuid = UUID.randomUUID()
        return try {
            createTableIfNotExists(table)
            val asyncResultSetCompletionStage = databaseRequestRetry {
                val statement =
                    SimpleStatement.newInstance(
                        "INSERT INTO $keyspace.$table (partitionKey, id, json, time) VALUES ($partitionKey, $uuid, '$row', toTimestamp(now()));"
                    ).setTracing(true)
                session!!.executeAsync(statement)
            }
            val resultSet = asyncResultSetCompletionStage?.await()
            logger.debug { "${resultSet?.executionInfo?.queryTrace?.parameters}\nTIME: ${resultSet?.executionInfo?.queryTrace?.durationMicros}\n" }
            uuid.toString()
        } catch (e: DriverTimeoutException) {
            logger.error(e) { e.message }
            null
        } catch (e: QueryExecutionException) {
            logger.error(e) { e.message }
            null
        } catch (e: QueryValidationException) {
            logger.error(e) { e.message }
            null
        }
    }

    suspend fun updateRecordInCollection(partitionKey: UUID, collection: String, id: String, updatedRecord: String) {
        try {
            val asyncResultSetCompletionStage = databaseRequestRetry {
                val statement =
                    SimpleStatement.newInstance(
                        "UPDATE $keyspace.$collection SET json = '$updatedRecord', time = toTimestamp(now()) WHERE partitionKey = $partitionKey AND id = $id;"
                    ).setTracing(true)
                session!!.executeAsync(statement)
            }
            val resultSet = asyncResultSetCompletionStage?.await()
            logger.debug { "${resultSet?.executionInfo?.queryTrace?.parameters}\nTIME: ${resultSet?.executionInfo?.queryTrace?.durationMicros}\n" }
        } catch (e: DriverTimeoutException) {
            logger.error(e) { e.message }
        } catch (e: QueryExecutionException) {
            logger.error(e) { e.message }
        } catch (e: QueryValidationException) {
            logger.error(e) { e.message }
        }
    }

    suspend fun isIdExistsInCollection(partitionKey: UUID, collection: String?, id: String?): Boolean {
        return getByIdFromCollection(partitionKey, collection, id) != null
    }

    suspend fun isTimestampExistsInCollection(
        partitionKey: UUID,
        collection: String?,
        id: String?,
        timestamp: String?
    ): Boolean {
        return getByIdAndTimestampFromCollection(partitionKey, collection, id, timestamp) != null
    }

    suspend fun deleteFromCollection(partitionKey: UUID, collection: String, id: String) {
        try {
            databaseRequestRetry {
                session!!.executeAsync("DELETE FROM $keyspace.$collection WHERE partitionKey = $partitionKey AND id = $id")
            }
        } catch (e: DriverTimeoutException) {
            logger.error(e) { e.message }
        } catch (e: QueryExecutionException) {
            logger.error(e) { e.message }
        } catch (e: QueryValidationException) {
            logger.error(e) { e.message }
        }
    }

    suspend fun clearTable(collection: String?) {
        try {
            databaseRequestRetry {
                session!!.executeAsync("TRUNCATE $keyspace.$collection")
            }
        } catch (e: DriverTimeoutException) {
            logger.error(e) { e.message }
        } catch (e: QueryExecutionException) {
            logger.error(e) { e.message }
        } catch (e: QueryValidationException) {
            logger.error(e) { e.message }
        }
    }

    suspend fun deleteRowsWithoutAlternateKeys(collection: String?) {
        try {
            val alternateKeysIds = getAlternateKeysIds(collection)
            if (alternateKeysIds.isEmpty()) {
                clearTable(collection)
            } else {
                val asyncResultSetCompletionStage = databaseRequestRetry {
                    val statement = SimpleStatement.newInstance("SELECT id FROM $keyspace.$collection").setTracing(true)
                    session!!.executeAsync(statement)
                }
                val resultSet = asyncResultSetCompletionStage?.await()
                val rows: MutableList<Row> = mutableListOf()
                logger.debug {
                    "${resultSet?.executionInfo?.queryTrace?.parameters}\n" +
                            "TIME: ${resultSet?.executionInfo?.queryTrace?.durationMicros}\n" +
                            "\n"
                }
                resultSet?.currentPage()?.forEach { row -> rows.add(row) }
                for (row in rows) {
                    val id = row.getObject("id").toString()
                    if (!alternateKeysIds.contains(id)) {
                        databaseRequestRetry {
                            session!!.execute("DELETE FROM $keyspace.$collection WHERE id = $id")
                        }
                    }
                }
            }
        } catch (e: DriverTimeoutException) {
            logger.error(e) { e.message }
        } catch (e: QueryExecutionException) {
            logger.error(e) { e.message }
        } catch (e: QueryValidationException) {
            logger.error(e) { e.message }
        }
    }

    private suspend fun getAlternateKeysIds(collection: String?): List<String> {
        return try {
            val ids: MutableList<String> = ArrayList()
            val asyncResultSetCompletionStage = databaseRequestRetry {
                val statement = SimpleStatement.newInstance(
                    "SELECT uuid_key FROM " + keyspace + "." + ALTERNATE_KEYS_STORAGE +
                            " WHERE collection = '" + collection + "' ALLOW FILTERING"
                ).setTracing(true)
                session!!.executeAsync(statement)
            }
            val resultSet = asyncResultSetCompletionStage?.await()
            val rows: MutableList<Row> = mutableListOf()
            logger.debug {
                "${resultSet?.executionInfo?.queryTrace?.parameters}\n" +
                        "TIME: ${resultSet?.executionInfo?.queryTrace?.durationMicros}\n" +
                        "\n"
            }
            resultSet?.currentPage()?.forEach { row -> rows.add(row) }
            for (row in rows) {
                ids.add(Objects.requireNonNull(row.getObject("uuid_key")).toString())
            }
            ids
        } catch (e: DriverTimeoutException) {
            logger.error(e) { e.message }
            emptyList()
        } catch (e: QueryExecutionException) {
            logger.error(e) { e.message }
            emptyList()
        } catch (e: QueryValidationException) {
            logger.error(e) { e.message }
            emptyList()
        }
    }

    suspend fun dropTable(collection: String) {
        try {
            databaseRequestRetry {
                session!!.executeAsync("DROP TABLE $keyspace.$collection")
            }
        } catch (e: DriverTimeoutException) {
            logger.error(e) { e.message }
        } catch (e: QueryExecutionException) {
            logger.error(e) { e.message }
        } catch (e: QueryValidationException) {
            logger.error(e) { e.message }
        }
    }

    suspend fun dropKeySpace(name: String) {
        try {
            databaseRequestRetry {
                session!!.executeAsync("DROP KEYSPACE $name")
            }
        } catch (e: DriverTimeoutException) {
            logger.error(e) { e.message }
        } catch (e: QueryExecutionException) {
            logger.error(e) { e.message }
        } catch (e: QueryValidationException) {
            logger.error(e) { e.message }
        }
    }

    fun close() {
        session!!.close()
    }

    init {
        val credentials = getDBCredentials("$FOLDER_PATH/$CRADLE_CONFIDENTIAL_FILE_NAME")!!
        host = credentials.host
        dataCenter = credentials.dataCenter
        username = credentials.username
        password = System.getenv(CASSANDRA_PASS)
        keyspace = getKeyspaceName("$FOLDER_PATH/$CUSTOM_JSON_FILE")
        port = credentials.port
    }
}