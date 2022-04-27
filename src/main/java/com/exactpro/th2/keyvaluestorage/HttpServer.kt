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

package com.exactpro.th2.keyvaluestorage


import io.ktor.application.*
import io.ktor.features.*
import io.ktor.gson.*
import io.ktor.http.*
import io.ktor.request.*
import io.ktor.response.*
import io.ktor.routing.*
import io.ktor.server.engine.*
import io.ktor.server.netty.*
import org.json.JSONObject
import java.text.DateFormat

class HttpServer {

    fun run(args: Array<String>) {
        embeddedServer(Netty, port = 8080) {
            install(ContentNegotiation) {
                gson {
                    setDateFormat(DateFormat.LONG)
                    setPrettyPrinting()
                }
            }

            routing {
                val cassandraConnector = CassandraConnector(args)
                cassandraConnector.connect { cassandraConnector.createKeySpaceIfNotExists() }

                get("/idsFromCollection") {
                    val parameters = call.request.queryParameters
                    if (parameters.contains(COLLECTION) && parameters.contains(USER_ID)) {
                        val collection = parameters[COLLECTION]?.toLowerCase()
                        if (cassandraConnector.isCollectionExists(collection)) {
                            val userId = parameters[USER_ID].toString()
                            if (cassandraConnector.checkUserIdExists(userId)) {
                                val partitionKey = cassandraConnector.getPartitionKeyByUserId(userId)
                                if (partitionKey != null) {
                                    val ids = cassandraConnector.getIdsFromCollection(collection)
                                    if (parameters.contains(SORT) && parameters[SORT].equals(ASCENDING)) {
                                        call.respond(HttpStatusCode.OK, ids)
                                    } else {
                                        call.respond(HttpStatusCode.OK, ids.reversed())
                                    }
                                } else {
                                    call.respond(HttpStatusCode.NotFound, "PartitionKey for userId $userId not found")
                                }
                            } else {
                                call.respond(
                                    HttpStatusCode.Forbidden,
                                    "User with id $userId not registered"
                                )
                            }
                        } else {
                            call.respond(HttpStatusCode.BadRequest, "Collection $collection not found")
                        }
                    } else {
                        call.respond(HttpStatusCode.BadRequest, "Request should contain collection and userId fields")
                    }
                }

                get("getUsers") {
                    var list: List<String> = cassandraConnector.selectAllFromCollection(USER)
                    call.respond(HttpStatusCode.OK, list.toString())
                }

                get("getKeys") {
                    var list: List<String> = cassandraConnector.selectAllFromCollection(USER_PARTITION_KEY_STORAGE)
                    call.respond(HttpStatusCode.OK, list.toString())
                }

                get("/allRecordsFromCollection") {
                    val parameters = call.request.queryParameters
                    if (parameters.contains(COLLECTION) && parameters.contains(USER_ID)) {
                        val collection = parameters[COLLECTION]?.toLowerCase()
                        if (cassandraConnector.isCollectionExists(collection)) {
                            val userId = parameters[USER_ID].toString()
                            if (cassandraConnector.checkUserIdExists(userId)) {
                                val partitionKey = cassandraConnector.getPartitionKeyByUserId(userId)
                                if (partitionKey != null) {
                                    val records =
                                        cassandraConnector.selectRecordsFromCollection(partitionKey, collection)
                                    if (parameters.contains(SORT) && parameters[SORT].equals(ASCENDING)) {
                                        call.respond(HttpStatusCode.OK, records.toString())
                                    } else {
                                        call.respond(HttpStatusCode.OK, records.reversed().toString())
                                    }
                                } else {
                                    call.respond(HttpStatusCode.NotFound, "PartitionKey for userId $userId not found")
                                }
                            } else {
                                call.respond(
                                    HttpStatusCode.Forbidden,
                                    "User with id $userId not registered"
                                )
                            }
                        } else {
                            call.respond(HttpStatusCode.NotFound, "Collection $collection not found")
                        }
                    } else {
                        call.respond(HttpStatusCode.BadRequest, "Request should contain collection and userId fields")
                    }
                }

                get("/getById") {
                    val parameters = call.request.queryParameters
                    if (parameters.contains(COLLECTION) && parameters.contains(ID) && parameters.contains(USER_ID)) {
                        val collection = parameters[COLLECTION]?.toLowerCase()
                        val id = parameters[ID]
                        if (cassandraConnector.isCollectionExists(collection)) {
                            val userId = parameters[USER_ID].toString()
                            if (cassandraConnector.checkUserIdExists(userId)) {
                                val partitionKey = cassandraConnector.getPartitionKeyByUserId(userId)
                                if (partitionKey != null) {
                                    val key = cassandraConnector.getCorrectId(partitionKey, collection, id)
                                    if (key != null && cassandraConnector.isIdExistsInCollection(
                                            partitionKey,
                                            collection,
                                            key
                                        )
                                    ) {
                                        if (parameters.contains(TIMESTAMP)) {
                                            val timestamp = parameters[TIMESTAMP].toString()
                                            if (cassandraConnector.isTimestampExistsInCollection(
                                                    partitionKey,
                                                    collection,
                                                    id,
                                                    timestamp
                                                )
                                            ) {
                                                var result = cassandraConnector.getByIdAndTimestampFromCollection(
                                                    partitionKey,
                                                    collection,
                                                    id,
                                                    timestamp
                                                )
                                                call.respond(HttpStatusCode.OK, result.toString())
                                            } else {
                                                call.respond(
                                                    HttpStatusCode.NotFound,
                                                    "Timestamp $timestamp in collection $collection not found"
                                                )
                                            }
                                        }
                                        var result =
                                            cassandraConnector.getByIdFromCollection(partitionKey, collection, key)
                                        call.respond(HttpStatusCode.OK, result.toString())
                                    } else {
                                        call.respond(
                                            HttpStatusCode.NotFound,
                                            "Id $id in collection $collection not found"
                                        )
                                    }
                                } else {
                                    call.respond(HttpStatusCode.NotFound, "PartitionKey for userId $userId not found")
                                }
                            } else {
                                call.respond(
                                    HttpStatusCode.Forbidden,
                                    "User with id $userId not registered"
                                )
                            }
                        } else {
                            call.respond(HttpStatusCode.NotFound, "Collection $collection not found")
                        }
                    } else {
                        call.respond(
                            HttpStatusCode.BadRequest,
                            "Request should contain userId, collection and id fields"
                        )
                    }
                }

                get("/") {
                    call.respond(HttpStatusCode.OK, "Hi! There is an empty response...")
                }

                get("/getWorkspaceLink") {
                    if (cassandraConnector.isCollectionExists(WORKSPACE_LINKS)) {
                        if (call.parameters.contains(ID) && call.parameters.contains(USER_ID)) {
                            val userId = call.parameters[USER_ID].toString()
                            if (cassandraConnector.checkUserIdExists(userId)) {
                                val partitionKey = cassandraConnector.getPartitionKeyByUserId(userId)
                                if (partitionKey != null) {
                                    val id = call.parameters[ID]
                                    if (cassandraConnector.isIdExistsInCollection(partitionKey, WORKSPACE_LINKS, id)) {
                                        val result =
                                            cassandraConnector.getByIdFromCollection(partitionKey, WORKSPACE_LINKS, id)
                                        call.respond(HttpStatusCode.OK, result.toString())
                                    } else {
                                        call.respond(
                                            HttpStatusCode.NotFound,
                                            "Id $id in collection $WORKSPACE_LINKS not found"
                                        )
                                    }
                                } else {
                                    call.respond(HttpStatusCode.NotFound, "PartitionKey for userId $userId not found")
                                }
                            } else {
                                call.respond(
                                    HttpStatusCode.Forbidden,
                                    "User with id $userId not registered"
                                )
                            }
                        } else {
                            call.respond(HttpStatusCode.NotFound, "Request should contain id and userId fields")
                        }
                    } else {
                        call.respond(HttpStatusCode.NotFound, "There are no saved links yet")
                    }
                }

                post("/store") {
                    val jsonData = JSONObject(call.receive<String>())
                    if (jsonData.length() > 0 && jsonData.has(COLLECTION) && jsonData.has(PAYLOAD) &&
                        jsonData.has(USER_ID)
                    ) {
                        val userId = jsonData.getString(USER_ID)
                        if (cassandraConnector.checkUserIdExists(userId)) {
                            val partitionKey = cassandraConnector.getPartitionKeyByUserId(userId)
                            if (partitionKey != null) {
                                val collection = jsonData[COLLECTION].toString().toLowerCase()
                                val uuid = cassandraConnector.insertIntoTable(
                                    partitionKey,
                                    collection,
                                    jsonData[PAYLOAD].toString()
                                )
                                if (jsonData.has(ID)) {
                                    val id = jsonData[ID].toString()
                                    cassandraConnector.createKeysTableIfNotExists()
                                    if (!cassandraConnector.isAlternateKeyExists(partitionKey, collection, id)) {
                                        cassandraConnector.setDefaultKey(collection, id, uuid.toString())
                                        call.respond(HttpStatusCode.OK, uuid.toString())
                                    } else {
                                        call.respond(HttpStatusCode.Conflict, "Object with id $id already exists")
                                    }
                                } else {
                                    call.respond(HttpStatusCode.OK, uuid.toString())
                                }
                            } else {
                                call.respond(HttpStatusCode.NotFound, "PartitionKey for userId $userId not found")
                            }
                        } else {
                            call.respond(
                                HttpStatusCode.Forbidden,
                                "User with id $userId not registered"
                            )
                        }
                    } else {
                        call.respond(
                            HttpStatusCode.BadRequest,
                            "Request should contain json with collection and payload fields"
                        )
                    }
                }

                post("/update") {
                    val jsonData = JSONObject(call.receive<String>())
                    if (jsonData.length() > 0 && jsonData.has(ID) && jsonData.has(COLLECTION) && jsonData.has(PAYLOAD)
                        && jsonData.has(USER_ID)
                    ) {
                        var id: String? = jsonData[ID].toString()
                        val collection = jsonData[COLLECTION].toString().toLowerCase()
                        if (cassandraConnector.isCollectionExists(collection)) {
                            val userId = jsonData.getString(USER_ID)
                            if (cassandraConnector.checkUserIdExists(userId)) {
                                val partitionKey = cassandraConnector.getPartitionKeyByUserId(userId)
                                if (partitionKey != null) {
                                    id = cassandraConnector.getCorrectId(partitionKey, collection, id)
                                    if (cassandraConnector.isIdExistsInCollection(
                                            partitionKey,
                                            collection,
                                            cassandraConnector.getCorrectId(partitionKey, collection, id)
                                        )
                                    ) {
                                        cassandraConnector.updateRecordInCollection(
                                            partitionKey,
                                            collection,
                                            id.toString(),
                                            jsonData[PAYLOAD].toString()
                                        )
                                        call.respond(HttpStatusCode.OK)
                                    } else {
                                        call.respond(
                                            HttpStatusCode.NotFound,
                                            "Id $id in collection $collection not found"
                                        )
                                    }
                                } else {
                                    call.respond(HttpStatusCode.NotFound, "PartitionKey for userId $userId not found")
                                }
                            } else {
                                call.respond(
                                    HttpStatusCode.Forbidden,
                                    "User with id $userId not registered"
                                )
                            }
                        } else {
                            call.respond(HttpStatusCode.NotFound, "Collection $collection not found")
                        }
                    } else {
                        call.respond(
                            HttpStatusCode.BadRequest,
                            "Request should contain json with collection, id and payload fields"
                        )
                    }
                }

                post("registerUser") {
                    val jsonData = JSONObject(call.receive<String>())
                    if (jsonData.length() > 0 && jsonData.has(NAME)
                    ) {
                        val name = jsonData.getString(NAME)
                        cassandraConnector.createUserTableIfNotExists()
                        cassandraConnector.createPartitionKeyStorageIfNotExists()
                        if (!cassandraConnector.checkUserNameExists(name)) {
                            val userId = cassandraConnector.registerUser(name)
                            if (!userId.isNullOrEmpty()) {
                                call.respond(HttpStatusCode.OK, userId)
                            } else {
                                call.respond(
                                    HttpStatusCode.Forbidden,
                                    "User registration error"
                                )
                            }
                        } else {
                            call.respond(
                                HttpStatusCode.Forbidden,
                                "Username $name is already taken"
                            )
                        }
                    } else {
                        call.respond(
                            HttpStatusCode.BadRequest,
                            "Request should contain json with name field"
                        )
                    }
                }

                delete("/delete") {
                    if (call.parameters.contains(ID) && call.parameters.contains(COLLECTION) && call.parameters.contains(
                            USER_ID
                        )
                    ) {
                        val collection = call.parameters[COLLECTION].toString().toLowerCase()
                        val id = call.parameters[ID].toString()
                        if (cassandraConnector.isCollectionExists(collection)) {
                            val userId = call.parameters[USER_ID].toString()
                            if (cassandraConnector.checkUserIdExists(userId)) {
                                val partitionKey = cassandraConnector.getPartitionKeyByUserId(userId)
                                if (partitionKey != null) {
                                    if (cassandraConnector.isValidUUID(id)) {
                                        if (cassandraConnector.isIdExistsInCollection(partitionKey, collection, id)) {
                                            cassandraConnector.deleteFromCollection(partitionKey, collection, id)
                                            call.respond(HttpStatusCode.OK)
                                        } else {
                                            call.respond(
                                                HttpStatusCode.NotFound,
                                                "Id $id in collection $collection not found"
                                            )
                                        }
                                    } else {
                                        call.respond(HttpStatusCode.NotFound, "Collection $collection not found")
                                    }
                                } else {
                                    call.respond(HttpStatusCode.NotFound, "PartitionKey for userId $userId not found")
                                }
                            } else {
                                call.respond(
                                    HttpStatusCode.Forbidden,
                                    "User with id $userId not registered"
                                )
                            }
                        } else {
                            call.respond(
                                HttpStatusCode.BadRequest,
                                "Request should contain collection and id fields"
                            )
                        }
                    }
                }

                delete("/dropCollection") {
                    if (call.parameters.contains(COLLECTION) && call.parameters.contains(USER_ID)) {
                        val collection = call.parameters[COLLECTION]?.toLowerCase()
                        if (cassandraConnector.isCollectionExists(collection)) {
                            cassandraConnector.clearTable(collection)
                            call.respond(HttpStatusCode.OK, "All records in collection $collection deleted")
                        } else {
                            call.respond(HttpStatusCode.NotFound, "Collection $collection not found")
                        }
                    } else {
                        call.respond(HttpStatusCode.NotFound, "Request should contain collection field")
                    }
                }

                delete("/clearCollection") {
                    if (call.parameters.contains(COLLECTION) && call.parameters.contains(USER_ID)) {
                        val collection = call.parameters[COLLECTION]?.toLowerCase()
                        if (cassandraConnector.isCollectionExists(collection)) {
                            cassandraConnector.deleteRowsWithoutAlternateKeys(collection)
                            call.respond(HttpStatusCode.OK, "All not default records in collection $collection deleted")
                        } else {
                            call.respond(HttpStatusCode.NotFound, "Collection $collection not found")
                        }
                    } else {
                        call.respond(HttpStatusCode.NotFound, "Request should contain collection field")
                    }
                }
            }
        }.start(wait = true)
    }
}
