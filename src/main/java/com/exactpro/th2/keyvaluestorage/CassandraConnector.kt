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
import com.datastax.oss.driver.api.core.cql.Row
import com.datastax.oss.driver.api.core.servererrors.QueryExecutionException
import com.datastax.oss.driver.api.core.servererrors.QueryValidationException
import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.ObjectMapper
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import mu.KotlinLogging
import org.apache.commons.cli.*
import java.io.File
import java.io.IOException
import java.math.BigInteger
import java.net.InetSocketAddress
import java.util.*
import java.util.regex.Pattern
import kotlin.system.exitProcess


class CassandraConnector(args: Array<String>) {
    private var FOLDER_PATH: String? = null
    private val host: String?
    private val dataCenter: String?
    private val username: String?
    private val password: String?
    private val keyspace: String?
    private val port: String?

    private val logger = KotlinLogging.logger {}

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

    private fun findJsonPlugin(name: String): String {
        val defaultPaths = listOf("/var/th2/config/", "src/main/resources", "src/test/resources", "/home", ".", "configs")
        for (path in defaultPaths) {
            for (file in File(path).walk()) {
                if (file.name.contains(name)) {
                    logger.debug { "FOLDER PATH: ${file.parent}" }
                    return file.parent
                }
            }
        }
        throw IOException("Cannot find plugin '$name' from paths: $defaultPaths")
    }

    private fun getKeyspaceName(keyspaceNameStoragePath: String): String? {
        return try {
            val objectMapper = ObjectMapper()
            val jsonMap = objectMapper.readValue(File(keyspaceNameStoragePath), HashMap::class.java)
            return jsonMap["keyspace"].toString()
        } catch (e: IOException) {
            logger.error(e) { "ERROR" }
            null
        }
    }

    private fun getDBCredentials(jsonCredentialsPath: String): DBCredentials? {
        return try {
            val objectMapper = ObjectMapper()
            objectMapper.readValue(File(jsonCredentialsPath), DBCredentials::class.java)
        } catch (e: IOException) {
            logger.error(e) { "ERROR" }
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

    suspend fun createKeySpaceIfNotExists() {
        try {
            databaseRequestRetry {
                session!!.execute("CREATE KEYSPACE IF NOT EXISTS $keyspace WITH replication = {'class':'SimpleStrategy', 'replication_factor' : 3}")
            }
        } catch (e: DriverTimeoutException) {
            logger.error(e) { "ERROR" }
        } catch (e: QueryExecutionException) {
            logger.error(e) { "ERROR" }
        } catch (e: QueryValidationException) {
            logger.error(e) { "ERROR" }
        }
    }

    suspend fun selectAllFromCollection(table: String?): List<String> {
        return try {
            val rows =
                databaseRequestRetry<List<Row>> { session!!.execute("SELECT * FROM $keyspace.$table").all() }
            val result: MutableList<String> = ArrayList()
            val records = sortByTimestamp(rows!!)
            for (record in records) {
                val mapper = ObjectMapper()
                result.add(mapper.writeValueAsString(record))
            }
            result
        } catch (e: DriverTimeoutException) {
            logger.error(e) { "ERROR" }
            emptyList()
        } catch (e: QueryExecutionException) {
            logger.error(e) { "ERROR" }
            emptyList()
        } catch (e: QueryValidationException) {
            logger.error(e) { "ERROR" }
            emptyList()
        } catch (e: JsonProcessingException) {
            logger.error(e) { "ERROR" }
            emptyList()
        }
    }

    suspend fun getCorrectId(collection: String?, id: String?): String? {
        return if (isValidUUID(id)) id.toString() else getUUIDByKeyName(collection, id)
    }

    private suspend fun createTableIfNotExists(table: String) {
        try {
            databaseRequestRetry {
                session!!.execute(
                    "CREATE TABLE IF NOT EXISTS " + keyspace + "." + table +
                            " (id uuid, " +
                            "json text, time bigint, " +
                            "PRIMARY KEY (id))"
                )
            }
        } catch (e: DriverTimeoutException) {
            logger.error(e) { "ERROR" }
        } catch (e: QueryExecutionException) {
            logger.error(e) { "ERROR" }
        } catch (e: QueryValidationException) {
            logger.error(e) { "ERROR" }
        }
    }

    suspend fun setDefaultKey(collection: String, keyName: String, uuidValue: String) {
        try {
            databaseRequestRetry {
                session!!.execute(
                    "INSERT INTO " +
                            keyspace + "." + ALTERNATE_KEYS_STORAGE + " (name, collection, uuid_key) " +
                            "VALUES ('" + keyName + "', '" + collection + "', " + uuidValue + ")"
                )
            }
        } catch (e: DriverTimeoutException) {
            logger.error(e) { "ERROR" }
        } catch (e: QueryExecutionException) {
            logger.error(e) { "ERROR" }
        } catch (e: QueryValidationException) {
            logger.error(e) { "ERROR" }
        }
    }

    suspend fun isAlternateKeyExists(collection: String, keyName: String): Boolean {
        return getUUIDByKeyName(collection, keyName) != null
    }

    private suspend fun getUUIDByKeyName(collection: String?, keyName: String?): String? {
        return try {
            val row = databaseRequestRetry {
                session!!.execute(
                    "SELECT * FROM " + keyspace + "." + ALTERNATE_KEYS_STORAGE +
                            " WHERE name = '" + keyName + "' AND collection = '" + collection + "'"
                ).one()
            }
            row?.getObject("uuid_key")?.toString().toString()
        } catch (e: DriverTimeoutException) {
            logger.error(e) { "ERROR" }
            null
        } catch (e: QueryExecutionException) {
            logger.error(e) { "ERROR" }
            null
        } catch (e: QueryValidationException) {
            logger.error(e) { "ERROR" }
            null
        }
    }

    suspend fun createKeysTableIfNotExists() {
        try {
            databaseRequestRetry {
                session!!.execute(
                    "CREATE TABLE IF NOT EXISTS " + keyspace + ".alternate_keys_storage (name text, collection text, " +
                            "uuid_key uuid, " +
                            "PRIMARY KEY (name, collection))"
                )
            }
        } catch (e: DriverTimeoutException) {
            logger.error(e) { "ERROR" }
        } catch (e: QueryExecutionException) {
            logger.error(e) { "ERROR" }
        } catch (e: QueryValidationException) {
            logger.error(e) { "ERROR" }
        }
    }

    suspend fun getByIdAndTimestampFromCollection(collection: String?, id: String?, timestamp: String?): String? {
        return try {
            val row = databaseRequestRetry {
                session!!.execute(
                    "SELECT * FROM " + keyspace + "." + collection +
                            " WHERE id = " + id + " AND time = " + timestamp + " ALLOW FILTERING"
                ).one()
            }
            row?.getObject("json")?.toString()
        } catch (e: DriverTimeoutException) {
            logger.error(e) { "ERROR" }
            null
        } catch (e: QueryExecutionException) {
            logger.error(e) { "ERROR" }
            null
        } catch (e: QueryValidationException) {
            logger.error(e) { "ERROR" }
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

    suspend fun getByIdFromCollection(collection: String?, id: String?): String? {
        return try {
            val row =
                databaseRequestRetry { session!!.execute("SELECT * FROM $keyspace.$collection WHERE id = $id").one() }
            row?.getObject("json")?.toString()
        } catch (e: DriverTimeoutException) {
            logger.error(e) { "ERROR" }
            null
        } catch (e: QueryExecutionException) {
            logger.error(e) { "ERROR" }
            null
        } catch (e: QueryValidationException) {
            logger.error(e) { "ERROR" }
            null
        }
    }

    suspend fun getIdsFromCollection(table: String?): List<String> {
        return try {
            val idsList: MutableList<String> = ArrayList()
            val rows = databaseRequestRetry<List<Row>> { session!!.execute("SELECT * FROM $keyspace.$table").all() }
            val records = sortByTimestamp(rows!!)
            for (record in records) {
                record.id.let { idsList.add(it) }
            }
            idsList
        } catch (e: DriverTimeoutException) {
            logger.error(e) { "ERROR" }
            emptyList()
        } catch (e: QueryExecutionException) {
            logger.error(e) { "ERROR" }
            emptyList()
        } catch (e: QueryValidationException) {
            logger.error(e) { "ERROR" }
            emptyList()
        }
    }

    private suspend fun allTables(): List<String> {
        return try {
            val tables: MutableList<String> = ArrayList()
            val rows = databaseRequestRetry<List<Row>> {
                session!!.execute("SELECT * FROM system_schema.tables WHERE keyspace_name = '$keyspace'").all()
            }
            for (row in rows!!) {
                tables.add(Objects.requireNonNull(row.getObject("table_name")).toString())
            }
            tables
        } catch (e: DriverTimeoutException) {
            logger.error(e) { "ERROR" }
            emptyList()
        } catch (e: QueryExecutionException) {
            logger.error(e) { "ERROR" }
            emptyList()
        } catch (e: QueryValidationException) {
            logger.error(e) { "ERROR" }
            emptyList()
        }
    }

    suspend fun isCollectionExists(table: String?): Boolean = allTables().contains(table)

    suspend fun insertIntoTable(table: String, `object`: String): String? {
        val uuid = UUID.randomUUID()
        return try {
            createTableIfNotExists(table)
            databaseRequestRetry {
                session!!.execute(
                    "INSERT INTO " + keyspace + "." + table +
                            " (id,json,time) VALUES (" + uuid + ", '" + `object` + "', toTimestamp(now()))"
                )
            }
            uuid.toString()
        } catch (e: DriverTimeoutException) {
            logger.error(e) { "ERROR" }
            null
        } catch (e: QueryExecutionException) {
            logger.error(e) { "ERROR" }
            null
        } catch (e: QueryValidationException) {
            logger.error(e) { "ERROR" }
            null
        }
    }

    suspend fun updateRecordInCollection(collection: String, id: String, updatedRecord: String) {
        try {
            databaseRequestRetry {
                session!!.execute(
                    "UPDATE " + keyspace + "." + collection +
                            " SET json = '" + updatedRecord + "', time = toTimestamp(now()) WHERE id = " + id
                )
            }
        } catch (e: DriverTimeoutException) {
            logger.error(e) { "ERROR" }
        } catch (e: QueryExecutionException) {
            logger.error(e) { "ERROR" }
        } catch (e: QueryValidationException) {
            logger.error(e) { "ERROR" }
        }
    }

    suspend fun isIdExistsInCollection(collection: String?, id: String?): Boolean {
        return getByIdFromCollection(collection, id) != null
    }

    suspend fun isTimestampExistsInCollection(collection: String?, id: String?, timestamp: String?): Boolean {
        return getByIdAndTimestampFromCollection(collection, id, timestamp) != null
    }

    suspend fun deleteFromCollection(collection: String, id: String) {
        try {
            databaseRequestRetry {
                session!!.execute("DELETE FROM $keyspace.$collection WHERE id = $id")
            }
        } catch (e: DriverTimeoutException) {
            logger.error(e) { "ERROR" }
        } catch (e: QueryExecutionException) {
            logger.error(e) { "ERROR" }
        } catch (e: QueryValidationException) {
            logger.error(e) { "ERROR" }
        }
    }

    suspend fun clearTable(collection: String?) {
        try {
            databaseRequestRetry {
                session!!.execute("TRUNCATE $keyspace.$collection")
            }
        } catch (e: DriverTimeoutException) {
            logger.error(e) { "ERROR" }
        } catch (e: QueryExecutionException) {
            logger.error(e) { "ERROR" }
        } catch (e: QueryValidationException) {
            logger.error(e) { "ERROR" }
        }
    }

    suspend fun deleteRowsWithoutAlternateKeys(collection: String?) {
        try {
            val alternateKeysIds = getAlternateKeysIds(collection)
            if (alternateKeysIds.isEmpty()) {
                clearTable(collection)
            } else {
                val rows = databaseRequestRetry<List<Row>> {
                    session!!.execute("SELECT id FROM $keyspace.$collection").all()
                }
                for (row in rows!!) {
                    val id = row.getObject("id").toString()
                    if (!alternateKeysIds.contains(id)) {
                        databaseRequestRetry {
                            session!!.execute("DELETE FROM $keyspace.$collection WHERE id = $id")
                        }
                    }
                }
            }
        } catch (e: DriverTimeoutException) {
            logger.error(e) { "ERROR" }
        } catch (e: QueryExecutionException) {
            logger.error(e) { "ERROR" }
        } catch (e: QueryValidationException) {
            logger.error(e) { "ERROR" }
        }
    }

    private suspend fun getAlternateKeysIds(collection: String?): List<String> {
        return try {
            val ids: MutableList<String> = ArrayList()
            val rows = databaseRequestRetry<List<Row>> {
                session!!.execute(
                    "SELECT uuid_key FROM " + keyspace + "." + ALTERNATE_KEYS_STORAGE +
                            " WHERE collection = '" + collection + "' ALLOW FILTERING"
                ).all()
            }
            for (row in rows!!) {
                ids.add(Objects.requireNonNull(row.getObject("uuid_key")).toString())
            }
            ids
        } catch (e: DriverTimeoutException) {
            logger.error(e) { "ERROR" }
            emptyList()
        } catch (e: QueryExecutionException) {
            logger.error(e) { "ERROR" }
            emptyList()
        } catch (e: QueryValidationException) {
            logger.error(e) { "ERROR" }
            emptyList()
        }
    }

    suspend fun dropTable(collection: String) {
        try {
            databaseRequestRetry {
                session!!.execute("DROP TABLE $keyspace.$collection")
            }
        } catch (e: DriverTimeoutException) {
            logger.error(e) { "ERROR" }
        } catch (e: QueryExecutionException) {
            logger.error(e) { "ERROR" }
        } catch (e: QueryValidationException) {
            logger.error(e) { "ERROR" }
        }
    }

    suspend fun dropKeySpace(name: String) {
        try {
            databaseRequestRetry {
                session!!.execute("DROP KEYSPACE $name")
            }
        } catch (e: DriverTimeoutException) {
            logger.error(e) { "ERROR" }
        } catch (e: QueryExecutionException) {
            logger.error(e) { "ERROR" }
        } catch (e: QueryValidationException) {
            logger.error(e) { "ERROR" }
        }
    }

    fun close() {
        session!!.close()
    }

    init {
        FOLDER_PATH = findJsonPlugin(CUSTOM_JSON_FILE)
        val credentials = getDBCredentials("$FOLDER_PATH/$CRADLE_CONFIDENTIAL_FILE_NAME")!!
        host = credentials.host
        dataCenter = credentials.dataCenter
        username = credentials.username
        password = credentials.password
        keyspace = getKeyspaceName("$FOLDER_PATH/$CUSTOM_JSON_FILE")
        port = credentials.port
        println(credentials)
    }
}