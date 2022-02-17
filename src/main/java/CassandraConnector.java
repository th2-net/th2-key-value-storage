/*******************************************************************************
 * Copyright 2020-2022 Exactpro (Exactpro Systems Limited)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/

import com.datastax.oss.driver.api.core.*;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.servererrors.QueryExecutionException;
import com.datastax.oss.driver.api.core.servererrors.QueryValidationException;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.commons.cli.*;

import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.net.InetSocketAddress;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.regex.Pattern;

public class CassandraConnector {

    String ALTERNATE_KEYS_STORAGE = "alternate_keys_storage";
    String FOLDER_PATH;
    private static final String CRADLE_CONFIDENTIAL_FILE_NAME = "cradle.json";

    private final String host;
    private final String dataCenter;
    private final String username;
    private final String password;
    private final String keyspace;
    private final String port;

    private void readCommandLineArgs(String[] args){
        Options options = new Options();
        Option configs = new Option("c", "configs", true, "configs folder path");
        configs.setRequired(true);
        options.addOption(configs);

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = null;
        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            System.exit(1);
        }
        FOLDER_PATH = cmd.getOptionValue("configs");
    }

    public CassandraConnector(String[] args) {
        readCommandLineArgs(args);
        DBCredentials credentials = GetDBCredentials(FOLDER_PATH + "/" + CRADLE_CONFIDENTIAL_FILE_NAME);
        assert credentials != null;
        this.host = credentials.host;
        this.dataCenter = credentials.dataCenter;
        this.username = credentials.username;
        this.password = credentials.password;
        this.keyspace = credentials.keyspace;
        this.port = credentials.port;
    }

    private DBCredentials GetDBCredentials(String jsonCredentialsPath) {
        try{
            ObjectMapper objectMapper = new ObjectMapper();
            return objectMapper.readValue(new File(jsonCredentialsPath), DBCredentials.class);
        } catch (IOException e){
            System.out.println(e.getMessage());
            return null;
        }

    }

    private final Pattern UUID_REGEX_PATTERN =
            Pattern.compile("^[{]?[0-9a-fA-F]{8}-([0-9a-fA-F]{4}-){3}[0-9a-fA-F]{12}[}]?$");

    public boolean isValidUUID(String str) {
        if (str == null) {
            return false;
        }
        return UUID_REGEX_PATTERN.matcher(str).matches();
    }

    CqlSession session;

    public void connect() {
        session = CqlSession.builder()
                .addContactPoint(
                        new InetSocketAddress(host, Integer.parseInt(port)))
                .withLocalDatacenter(dataCenter)
                .withAuthCredentials(username, password)
                .build();
    }

    public void createKeySpace(String name) {
        try {
            session.execute("CREATE KEYSPACE " + name + " WITH replication = {'class':'SimpleStrategy', 'replication_factor' : 3}");
        } catch (DriverTimeoutException | QueryExecutionException | QueryValidationException e) {
            System.out.println(e.getMessage());
        }
    }

    public List<String> selectAllFromCollection(String table) {
        try {
            List<Row> rows = session.execute("SELECT * FROM " + keyspace + "." + table).all();
            List<String> result = new ArrayList<>();
            List<Record> records = sortByTimestamp(rows);
            for (Record record : records) {
                ObjectMapper mapper = new ObjectMapper();
                result.add(mapper.writeValueAsString(record));
            }
            return result;
        } catch (DriverTimeoutException | QueryExecutionException | QueryValidationException | JsonProcessingException e) {
            System.out.println(e.getMessage());
            return null;
        }
    }

    public String getCorrectId(String collection, String id) {
        return isValidUUID(id) ? id : getUUIDByKeyName(collection, id);
    }

    public void createTableIfNotExists(String table) {
        try {
            session.execute("CREATE TABLE IF NOT EXISTS " + keyspace + "." + table +
                    " (id uuid, " +
                    "json text, time bigint, " +
                    "PRIMARY KEY (id))");
        } catch (DriverTimeoutException | QueryExecutionException | QueryValidationException e) {
            System.out.println(e.getMessage());
        }
    }

    public void setDefaultKey(String collection, String keyName, String uuidValue) {
        try {
            session.execute("INSERT INTO " +
                    keyspace + "." + ALTERNATE_KEYS_STORAGE + " (name, collection, uuid_key) " +
                    "VALUES ('" + keyName + "', '" + collection + "', " + uuidValue + ")");
        } catch (DriverTimeoutException | QueryExecutionException | QueryValidationException e) {
            System.out.println(e.getMessage());
        }
    }

    public boolean isAlternateKeyExists(String collection, String keyName) {
        return getUUIDByKeyName(collection, keyName) != null;
    }

    @Nullable
    public String getUUIDByKeyName(String collection, String keyName) {
        try {
            Row row = session.execute("SELECT * FROM " + keyspace + "." + ALTERNATE_KEYS_STORAGE +
                    " WHERE name = '" + keyName + "' AND collection = '" + collection + "'").one();
            return row != null ? row.getObject("uuid_key").toString() : null;
        } catch (DriverTimeoutException | QueryExecutionException | QueryValidationException e) {
            System.out.println(e.getMessage());
            return null;
        }
    }

    public void createKeysTableIfNotExists() {
        try {
            session.execute("CREATE TABLE IF NOT EXISTS " + keyspace + ".alternate_keys_storage (name text, collection text, " +
                    "uuid_key uuid, " +
                    "PRIMARY KEY (name, collection))");
        } catch (DriverTimeoutException | QueryExecutionException | QueryValidationException e) {
            System.out.println(e.getMessage());
        }
    }

    public String getByIdAndTimestampFromCollection(String collection, String id, String timestamp){
        try {
            Row row = session.execute("SELECT * FROM " + keyspace + "." + collection +
                    " WHERE id = " + id + " AND time = " + timestamp + " ALLOW FILTERING").one();
            return row != null ? row.getObject("json").toString() : null;
        } catch (DriverTimeoutException | QueryExecutionException | QueryValidationException e) {
            System.out.println(e.getMessage());
            return null;
        }
    }

    public List<Record> sortByTimestamp(List<Row> records){
        List<Record> comparableRecords = new ArrayList<>();
        for (Row row : records) {
            Record record = new Record(row.getObject("id").toString(),
                    row.getObject("json").toString(), new BigInteger(row.getObject("time").toString()));
            comparableRecords.add(record);
        }
        comparableRecords.sort(Record.Comparators.TIME);
        return comparableRecords;
    }

    public String getByIdFromCollection(String collection, String id) {
        try {
            Row row = session.execute("SELECT * FROM " + keyspace + "." + collection + " WHERE id = " + id).one();
            return row != null ? row.getObject("json").toString() : null;
        } catch (DriverTimeoutException | QueryExecutionException | QueryValidationException e) {
            System.out.println(e.getMessage());
            return null;
        }
    }

    public List<String> getIdsFromCollection(String table) {
        try {
            List<String> idsList = new ArrayList<>();
            List<Row> rows = session.execute("SELECT * FROM " + keyspace + "." + table).all();
            List<Record> records = sortByTimestamp(rows);
            for (Record record : records) {
                idsList.add(record.id);
            }
            return idsList;
        } catch (DriverTimeoutException | QueryExecutionException | QueryValidationException e) {
            System.out.println(e.getMessage());
            return null;
        }
    }

    @Nullable
    public List<String> getAllTables() {
        try {
            List<String> tables = new ArrayList<>();
            List<Row> rows = session.execute("SELECT * FROM system_schema.tables WHERE keyspace_name = '" + keyspace + "'").all();
            for (Row row : rows) {
                tables.add(Objects.requireNonNull(row.getObject("table_name")).toString());
            }
            return tables;
        } catch (DriverTimeoutException | QueryExecutionException | QueryValidationException e) {
            System.out.println(e.getMessage());
            return null;
        }
    }

    public boolean isCollectionExists(String table) {
        List<String> tableNames = getAllTables();
        return tableNames != null && tableNames.contains(table);
    }

    public String insertIntoTable(String table, String object) {
        UUID uuid = UUID.randomUUID();
        try {
            createTableIfNotExists(table);
            session.execute("INSERT INTO " + keyspace + "." + table +
                    " (id,json,time) VALUES (" + uuid + ", '" + object + "', toTimestamp(now()))");
            return uuid.toString();
        } catch (DriverTimeoutException | QueryExecutionException | QueryValidationException e) {
            System.out.println(e.getMessage());
            return null;
        }
    }

    public void updateRecordInCollection(String collection, String id, String object) {
        try {
            session.execute("UPDATE " + keyspace + "." + collection +
                    " SET json = '" + object + "', time = toTimestamp(now()) WHERE id = " + id);
        } catch (DriverTimeoutException | QueryExecutionException | QueryValidationException e) {
            System.out.println(e.getMessage());
        }
    }

    public boolean isIdExistsInCollection(String collection, String id)
    {
        return getByIdFromCollection(collection, id) != null;
    }

    public boolean isTimestampExistsInCollection(String collection, String id, String timestamp) {
        return getByIdAndTimestampFromCollection(collection, id, timestamp) != null;
    }

    public void deleteFromCollection(String collection, String id) {
        try {
            session.execute("DELETE FROM " + keyspace + "." + collection + " WHERE id = " + id);
        } catch (DriverTimeoutException | QueryExecutionException | QueryValidationException e) {
            System.out.println(e.getMessage());
        }
    }

    public void clearTable(String collection) {
        try {
            session.execute("TRUNCATE " + keyspace + "." + collection);
        } catch (DriverTimeoutException | QueryExecutionException | QueryValidationException e) {
            System.out.println(e.getMessage());
        }
    }

    public void deleteRowsWithoutAlternateKeys(String collection) {
        try {
            List<String> alternateKeysIds = getAlternateKeysIds(collection);
            if (alternateKeysIds == null || alternateKeysIds.isEmpty()) {
                clearTable(collection);
            } else {
                List<Row> rows = session.execute("SELECT id FROM " + keyspace + "." + collection).all();
                for (Row row : rows) {
                    String id = row.getObject("id").toString();
                    if (!alternateKeysIds.contains(id)) {
                        session.execute("DELETE FROM " + keyspace + "." + collection + " WHERE id = " + id + "");
                    }
                }
            }
        } catch (DriverTimeoutException | QueryExecutionException | QueryValidationException e) {
            System.out.println(e.getMessage());
        }
    }

    private List<String> getAlternateKeysIds(String collection) {
        try {
            List<String> ids = new ArrayList<>();
            List<Row> rows = session.execute("SELECT uuid_key FROM " + keyspace + "." + ALTERNATE_KEYS_STORAGE +
                    " WHERE collection = '" + collection + "' ALLOW FILTERING").all();
            for (Row row : rows) {
                ids.add(Objects.requireNonNull(row.getObject("uuid_key")).toString());
            }
            return ids;
        } catch (DriverTimeoutException | QueryExecutionException | QueryValidationException e) {
            System.out.println(e.getMessage());
            return null;
        }
    }

    public void dropTable(String collection) {
        try {
            session.execute("DROP TABLE " + keyspace + "." + collection);
        } catch (DriverTimeoutException | QueryExecutionException | QueryValidationException e) {
            System.out.println(e.getMessage());
        }
    }

    public void dropKeySpace(String name) {
        try {
            session.execute("DROP KEYSPACE " + name);
        } catch (DriverTimeoutException | QueryExecutionException | QueryValidationException e) {
            System.out.println(e.getMessage());
        }
    }

    public void close() {
        session.close();
    }
}
