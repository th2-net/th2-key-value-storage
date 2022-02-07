/*******************************************************************************
 * Copyright 2020-2021 Exactpro (Exactpro Systems Limited)
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
import org.jetbrains.annotations.NotNull;

import java.io.FileNotFoundException;
import java.net.InetSocketAddress;

import com.datastax.oss.driver.api.core.cql.ResultSet;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.regex.Pattern;

public class CassandraConnector {

    @NotNull
    String databaseAddress = "th2-qa";
    String datacenterName = "datacenter1";
    String user = "th2";
    String password = "RkFosP24";
    String keyspace = "json_storage";

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
                        new InetSocketAddress(databaseAddress, 32110))
                .withLocalDatacenter(datacenterName)
                .withAuthCredentials(user, password)
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
            List<String> records = new ArrayList<>();
            for (Row row : rows) {
                records.add(row.getFormattedContents());
            }
            return records;
        } catch (DriverTimeoutException | QueryExecutionException | QueryValidationException e) {
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
                    "json text, " +
                    "PRIMARY KEY (id))");
        } catch (DriverTimeoutException | QueryExecutionException | QueryValidationException e) {
            System.out.println(e.getMessage());
        }
    }

    public void setKeyName(String table, String keyName) {
        try {
            String key = table + "_" + keyName;
            UUID uuid = UUID.randomUUID();
            ResultSet resultSet = session.execute("INSERT INTO " +
                    keyspace + ".alternate_keys_storage (name, uuid_key) VALUES ('" + key + "', " + uuid + ")");
        } catch (DriverTimeoutException | QueryExecutionException | QueryValidationException e) {
            System.out.println(e.getMessage());
        }
    }

    @Nullable
    public String getUUIDByKeyName(String collection, String keyName) {
        try {
            String name = collection + "_" + keyName;
            Row row = session.execute("SELECT * FROM " + keyspace + ".alternate_keys_storage WHERE name = '" + name + "'").one();
            return row != null ? row.getObject("uuid_key").toString() : null;
        } catch (DriverTimeoutException | QueryExecutionException | QueryValidationException e) {
            System.out.println(e.getMessage());
            return null;
        }
    }

    public void createKeysTableIfNotExists() {
        try {
            session.execute("CREATE TABLE IF NOT EXISTS " + keyspace + ".alternate_keys_storage (name text, " +
                    "uuid_key uuid, " +
                    "PRIMARY KEY (name))");
        } catch (DriverTimeoutException | QueryExecutionException | QueryValidationException e) {
            System.out.println(e.getMessage());
        }
    }

    public String getByIdFromCollection(String table, String id) {
        try {
            Row row = session.execute("SELECT * FROM " + keyspace + "." + table + " WHERE id = " + id).one();
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
            for (Row row : rows) {
                idsList.add(row.getObject("id").toString());
            }
            return idsList;
        } catch (DriverTimeoutException | QueryExecutionException | QueryValidationException e) {
            System.out.println(e.getMessage());
            return null;
        }
    }

    @Nullable
    private List<String> getAllTables() {
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
                    " (id,json) VALUES (" + uuid + ", '" + object + "')");
            return uuid.toString();
        } catch (DriverTimeoutException | QueryExecutionException | QueryValidationException e) {
            System.out.println(e.getMessage());
            return null;
        }
    }

    public void updateRecordInTable(String table, String id, String object) {
        try {
            session.execute("UPDATE " + keyspace + "." + table +
                    " SET json = '" + object + "' WHERE id = " + id);
        } catch (DriverTimeoutException | QueryExecutionException | QueryValidationException e) {
            System.out.println(e.getMessage());
        }
    }

    public boolean isIdExistsInCollection(String collection, String id) {
        return getByIdFromCollection(collection, id) != null;
    }

    public void deleteFromCollection(String collection, String id) {
        try {
            session.execute("DELETE FROM " + keyspace + "." + collection + " WHERE id = " + id);
        } catch (DriverTimeoutException | QueryExecutionException | QueryValidationException e) {
            System.out.println(e.getMessage());
        }
    }

    public void dropTable(String table) {
        try {
            session.execute("DROP TABLE " + keyspace + "." + table);
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
