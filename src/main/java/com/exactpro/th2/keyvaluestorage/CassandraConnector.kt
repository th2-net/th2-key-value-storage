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

import com.fasterxml.jackson.databind.ObjectMapper
import java.io.IOException
import com.datastax.oss.driver.api.core.CqlSession
import java.net.InetSocketAddress
import com.datastax.oss.driver.api.core.DriverTimeoutException
import com.datastax.oss.driver.api.core.cql.Row
import com.datastax.oss.driver.api.core.servererrors.QueryExecutionException
import com.datastax.oss.driver.api.core.servererrors.QueryValidationException
import com.fasterxml.jackson.core.JsonProcessingException
import org.apache.commons.cli.*
import java.io.File
import java.math.BigInteger
import java.util.*
import java.util.regex.Pattern

class CassandraConnector(args: Array<String>) {
    private var FOLDER_PATH: String? = null
    private val host: String?
    private val dataCenter: String?
    private val username: String?
    private val password: String?
    private val keyspace: String?
    private val port: String?
    private fun readCommandLineArgs(args: Array<String>) {
        val options = Options()
        val configs = Option("c", "configs", true, "configs folder path")
        configs.isRequired = true
        options.addOption(configs)
        val parser: CommandLineParser = DefaultParser()
        var cmd: CommandLine? = null
        try {
            cmd = parser.parse(options, args)
        } catch (e: ParseException) {
            println(e.message)
            System.exit(1)
        }
        FOLDER_PATH = cmd!!.getOptionValue("configs")
    }

    private fun GetDBCredentials(jsonCredentialsPath: String): DBCredentials? {
        return try {
            val objectMapper = ObjectMapper()
            objectMapper.readValue(File(jsonCredentialsPath), DBCredentials::class.java)
        } catch (e: IOException) {
            println(e.message)
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
    fun connect() {
        session = CqlSession.builder()
            .addContactPoint(
                InetSocketAddress(host, port!!.toInt())
            )
            .withLocalDatacenter(dataCenter!!)
            .withAuthCredentials(username!!, password!!)
            .build()
    }

    fun createKeySpace(name: String) {
        try {
            session!!.execute("CREATE KEYSPACE $name WITH replication = {'class':'SimpleStrategy', 'replication_factor' : 3}")
        } catch (e: DriverTimeoutException) {
            println(e.message)
        } catch (e: QueryExecutionException) {
            println(e.message)
        } catch (e: QueryValidationException) {
            println(e.message)
        }
    }

    fun selectAllFromCollection(table: String?): List<String> {
        return try {
            val rows = session!!.execute("SELECT * FROM $keyspace.$table").all()
            val result: MutableList<String> = ArrayList()
            val records = sortByTimestamp(rows)
            for (record in records) {
                val mapper = ObjectMapper()
                result.add(mapper.writeValueAsString(record))
            }
            result
        } catch (e: DriverTimeoutException) {
            println(e.message)
            emptyList()
        } catch (e: QueryExecutionException) {
            println(e.message)
            emptyList()
        } catch (e: QueryValidationException) {
            println(e.message)
            emptyList()
        } catch (e: JsonProcessingException) {
            println(e.message)
            emptyList()
        }
    }

    fun getCorrectId(collection: String?, id: String?): String {
        return if (isValidUUID(id)) id.toString() else getUUIDByKeyName(collection, id)!!
    }

    fun createTableIfNotExists(table: String) {
        try {
            session!!.execute(
                "CREATE TABLE IF NOT EXISTS " + keyspace + "." + table +
                        " (id uuid, " +
                        "json text, time bigint, " +
                        "PRIMARY KEY (id))"
            )
        } catch (e: DriverTimeoutException) {
            println(e.message)
        } catch (e: QueryExecutionException) {
            println(e.message)
        } catch (e: QueryValidationException) {
            println(e.message)
        }
    }

    fun setDefaultKey(collection: String, keyName: String, uuidValue: String) {
        try {
            session!!.execute(
                "INSERT INTO " +
                        keyspace + "." + ALTERNATE_KEYS_STORAGE + " (name, collection, uuid_key) " +
                        "VALUES ('" + keyName + "', '" + collection + "', " + uuidValue + ")"
            )
        } catch (e: DriverTimeoutException) {
            println(e.message)
        } catch (e: QueryExecutionException) {
            println(e.message)
        } catch (e: QueryValidationException) {
            println(e.message)
        }
    }

    fun isAlternateKeyExists(collection: String, keyName: String): Boolean {
        return getUUIDByKeyName(collection, keyName) != null
    }

    fun getUUIDByKeyName(collection: String?, keyName: String?): String {
        return try {
            val row = session!!.execute(
                "SELECT * FROM " + keyspace + "." + ALTERNATE_KEYS_STORAGE +
                        " WHERE name = '" + keyName + "' AND collection = '" + collection + "'"
            ).one()
            row?.getObject("uuid_key")?.toString().toString()
        } catch (e: DriverTimeoutException) {
            println(e.message)
            null.toString()
        } catch (e: QueryExecutionException) {
            println(e.message)
            null.toString()
        } catch (e: QueryValidationException) {
            println(e.message)
            null.toString()
        }
    }

    fun createKeysTableIfNotExists() {
        try {
            session!!.execute(
                "CREATE TABLE IF NOT EXISTS " + keyspace + ".alternate_keys_storage (name text, collection text, " +
                        "uuid_key uuid, " +
                        "PRIMARY KEY (name, collection))"
            )
        } catch (e: DriverTimeoutException) {
            println(e.message)
        } catch (e: QueryExecutionException) {
            println(e.message)
        } catch (e: QueryValidationException) {
            println(e.message)
        }
    }

    fun getByIdAndTimestampFromCollection(collection: String?, id: String?, timestamp: String?): String? {
        return try {
            val row = session!!.execute(
                "SELECT * FROM " + keyspace + "." + collection +
                        " WHERE id = " + id + " AND time = " + timestamp + " ALLOW FILTERING"
            ).one()
            row?.getObject("json")?.toString()
        } catch (e: DriverTimeoutException) {
            println(e.message)
            null
        } catch (e: QueryExecutionException) {
            println(e.message)
            null
        } catch (e: QueryValidationException) {
            println(e.message)
            null
        }
    }

    fun sortByTimestamp(records: List<Row>): List<Record> {
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

    fun getByIdFromCollection(collection: String?, id: String?): String? {
        return try {
            val row = session!!.execute("SELECT * FROM $keyspace.$collection WHERE id = $id").one()
            row?.getObject("json")?.toString()
        } catch (e: DriverTimeoutException) {
            println(e.message)
            null
        } catch (e: QueryExecutionException) {
            println(e.message)
            null
        } catch (e: QueryValidationException) {
            println(e.message)
            null
        }
    }

    fun getIdsFromCollection(table: String?): List<String> {
        return try {
            val idsList: MutableList<String> = ArrayList()
            val rows = session!!.execute("SELECT * FROM $keyspace.$table").all()
            val records = sortByTimestamp(rows)
            for (record in records) {
                record.id?.let { idsList.add(it) }
            }
            idsList
        } catch (e: DriverTimeoutException) {
            println(e.message)
            emptyList()
        } catch (e: QueryExecutionException) {
            println(e.message)
            emptyList()
        } catch (e: QueryValidationException) {
            println(e.message)
            emptyList()
        }
    }

    val allTables: List<String>?
        get() = try {
            val tables: MutableList<String> = ArrayList()
            val rows = session!!.execute("SELECT * FROM system_schema.tables WHERE keyspace_name = '$keyspace'").all()
            for (row in rows) {
                tables.add(Objects.requireNonNull(row.getObject("table_name")).toString())
            }
            tables
        } catch (e: DriverTimeoutException) {
            println(e.message)
            null
        } catch (e: QueryExecutionException) {
            println(e.message)
            null
        } catch (e: QueryValidationException) {
            println(e.message)
            null
        }

    fun isCollectionExists(table: String?): Boolean {
        val tableNames = allTables
        return tableNames != null && tableNames.contains(table)
    }

    fun insertIntoTable(table: String, `object`: String): String? {
        val uuid = UUID.randomUUID()
        return try {
            createTableIfNotExists(table)
            session!!.execute(
                "INSERT INTO " + keyspace + "." + table +
                        " (id,json,time) VALUES (" + uuid + ", '" + `object` + "', toTimestamp(now()))"
            )
            uuid.toString()
        } catch (e: DriverTimeoutException) {
            println(e.message)
            null
        } catch (e: QueryExecutionException) {
            println(e.message)
            null
        } catch (e: QueryValidationException) {
            println(e.message)
            null
        }
    }

    fun updateRecordInCollection(collection: String, id: String, `object`: String) {
        try {
            session!!.execute(
                "UPDATE " + keyspace + "." + collection +
                        " SET json = '" + `object` + "', time = toTimestamp(now()) WHERE id = " + id
            )
        } catch (e: DriverTimeoutException) {
            println(e.message)
        } catch (e: QueryExecutionException) {
            println(e.message)
        } catch (e: QueryValidationException) {
            println(e.message)
        }
    }

    fun isIdExistsInCollection(collection: String?, id: String?): Boolean {
        return getByIdFromCollection(collection, id) != null
    }

    fun isTimestampExistsInCollection(collection: String?, id: String?, timestamp: String?): Boolean {
        return getByIdAndTimestampFromCollection(collection, id, timestamp) != null
    }

    fun deleteFromCollection(collection: String, id: String) {
        try {
            session!!.execute("DELETE FROM $keyspace.$collection WHERE id = $id")
        } catch (e: DriverTimeoutException) {
            println(e.message)
        } catch (e: QueryExecutionException) {
            println(e.message)
        } catch (e: QueryValidationException) {
            println(e.message)
        }
    }

    fun clearTable(collection: String?) {
        try {
            session!!.execute("TRUNCATE $keyspace.$collection")
        } catch (e: DriverTimeoutException) {
            println(e.message)
        } catch (e: QueryExecutionException) {
            println(e.message)
        } catch (e: QueryValidationException) {
            println(e.message)
        }
    }

    fun deleteRowsWithoutAlternateKeys(collection: String?) {
        try {
            val alternateKeysIds = getAlternateKeysIds(collection)
            if (alternateKeysIds == null || alternateKeysIds.isEmpty()) {
                clearTable(collection)
            } else {
                val rows = session!!.execute("SELECT id FROM $keyspace.$collection").all()
                for (row in rows) {
                    val id = row.getObject("id").toString()
                    if (!alternateKeysIds.contains(id)) {
                        session!!.execute("DELETE FROM $keyspace.$collection WHERE id = $id")
                    }
                }
            }
        } catch (e: DriverTimeoutException) {
            println(e.message)
        } catch (e: QueryExecutionException) {
            println(e.message)
        } catch (e: QueryValidationException) {
            println(e.message)
        }
    }

    private fun getAlternateKeysIds(collection: String?): List<String> {
        return try {
            val ids: MutableList<String> = ArrayList()
            val rows = session!!.execute(
                "SELECT uuid_key FROM " + keyspace + "." + ALTERNATE_KEYS_STORAGE +
                        " WHERE collection = '" + collection + "' ALLOW FILTERING"
            ).all()
            for (row in rows) {
                ids.add(Objects.requireNonNull(row.getObject("uuid_key")).toString())
            }
            ids
        } catch (e: DriverTimeoutException) {
            println(e.message)
            emptyList()
        } catch (e: QueryExecutionException) {
            println(e.message)
            emptyList()
        } catch (e: QueryValidationException) {
            println(e.message)
            emptyList()
        }
    }

    fun dropTable(collection: String) {
        try {
            session!!.execute("DROP TABLE $keyspace.$collection")
        } catch (e: DriverTimeoutException) {
            println(e.message)
        } catch (e: QueryExecutionException) {
            println(e.message)
        } catch (e: QueryValidationException) {
            println(e.message)
        }
    }

    fun dropKeySpace(name: String) {
        try {
            session!!.execute("DROP KEYSPACE $name")
        } catch (e: DriverTimeoutException) {
            println(e.message)
        } catch (e: QueryExecutionException) {
            println(e.message)
        } catch (e: QueryValidationException) {
            println(e.message)
        }
    }

    fun close() {
        session!!.close()
    }

    companion object {
        private const val ALTERNATE_KEYS_STORAGE = "alternate_keys_storage"
        private const val CRADLE_CONFIDENTIAL_FILE_NAME = "cradle.json"
    }

    init {
        readCommandLineArgs(args)
        val credentials = GetDBCredentials(FOLDER_PATH + "/" + CRADLE_CONFIDENTIAL_FILE_NAME)!!
        host = credentials.host
        dataCenter = credentials.dataCenter
        username = credentials.username
        password = credentials.password
        keyspace = credentials.keyspace
        port = credentials.port
    }
}