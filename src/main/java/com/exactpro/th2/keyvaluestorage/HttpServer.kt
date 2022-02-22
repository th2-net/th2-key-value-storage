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
                cassandraConnector.connect()

                get("/idsFromCollection") {
                    val parameters = call.request.queryParameters
                    if (parameters.contains(COLLECTION)) {
                        val collection = parameters[COLLECTION]?.toLowerCase()
                        if (cassandraConnector.isCollectionExists(collection)) {
                            val ids = cassandraConnector.getIdsFromCollection(collection)
                            if (parameters.contains(SORT) && parameters[SORT].equals(ASCENDING)) {
                                call.respond(HttpStatusCode.OK, ids)
                            } else {
                                call.respond(HttpStatusCode.OK, ids.reversed())
                            }
                        } else {
                            call.respond(HttpStatusCode.BadRequest, "Collection $collection not found")
                        }
                    } else {
                        call.respond(HttpStatusCode.BadRequest, "Request should contain collection field")
                    }
                }

                get("/allRecordsFromCollection") {
                    val parameters = call.request.queryParameters
                    if (parameters.contains(COLLECTION)) {
                        val collection = parameters[COLLECTION]?.toLowerCase()
                        if (cassandraConnector.isCollectionExists(collection)) {
                            val records = cassandraConnector.selectAllFromCollection(collection)
                            if (parameters.contains(SORT) && parameters[SORT].equals(ASCENDING)) {
                                call.respond(HttpStatusCode.OK, records.toString())
                            } else {
                                call.respond(HttpStatusCode.OK, records.reversed().toString())
                            }
                        } else {
                            call.respond(HttpStatusCode.NotFound, "Collection $collection not found")
                        }
                    } else {
                        call.respond(HttpStatusCode.BadRequest, "Request should contain collection field")
                    }
                }

                get("/getById") {
                    val parameters = call.request.queryParameters
                    if (parameters.contains(COLLECTION) && parameters.contains(ID)) {
                        val collection = parameters[COLLECTION]?.toLowerCase()
                        val id = parameters[ID]
                        if (cassandraConnector.isCollectionExists(collection)) {
                            val key = cassandraConnector.getCorrectId(collection, id)
                            if (key != null) {
                                if (cassandraConnector.isIdExistsInCollection(collection, key)) {
                                    if (parameters.contains(TIMESTAMP)) {
                                        val timestamp = parameters[TIMESTAMP].toString()
                                        if (cassandraConnector.isTimestampExistsInCollection(
                                                collection,
                                                id,
                                                timestamp
                                            )
                                        ) {
                                            val result = cassandraConnector.getByIdAndTimestampFromCollection(
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
                                    val result = cassandraConnector.getByIdFromCollection(collection, key)
                                    call.respond(HttpStatusCode.OK, result.toString())
                                } else {
                                    call.respond(HttpStatusCode.NotFound, "Id $id in collection $collection not found")
                                }
                            } else {
                                call.respond(HttpStatusCode.NotFound, "Id $id in collection $collection not found")
                            }
                        } else {
                            call.respond(HttpStatusCode.NotFound, "Collection $collection not found")
                        }
                    } else {
                        call.respond(HttpStatusCode.BadRequest, "Request should contain collection and id fields")
                    }
                }

                get("/") {
                    call.respond(HttpStatusCode.OK, "Hi! There is an empty response...")
                }

                get("/getWorkspaceLink") {
                    if (cassandraConnector.isCollectionExists(WORKSPACE_LINKS)) {
                        if (call.parameters.contains(ID)) {
                            val id = call.parameters[ID]
                            if (cassandraConnector.isIdExistsInCollection(WORKSPACE_LINKS, id)) {
                                val result = cassandraConnector.getByIdFromCollection(WORKSPACE_LINKS, id)
                                call.respond(HttpStatusCode.OK, result.toString())
                            } else {
                                call.respond(HttpStatusCode.NotFound, "Id $id in collection $WORKSPACE_LINKS not found")
                            }
                        } else {
                            call.respond(HttpStatusCode.NotFound, "Request should contain id field")
                        }
                    } else {
                        call.respond(HttpStatusCode.NotFound, "There are no saved links yet")
                    }
                }

                post("/store") {
                    val jsonData = JSONObject(call.receive<String>())
                    if (jsonData.length() > 0 && jsonData.has(COLLECTION) && jsonData.has(PAYLOAD)) {
                        val collection = jsonData[COLLECTION].toString().toLowerCase()
                        val uuid = cassandraConnector.insertIntoTable(
                            collection,
                            jsonData[PAYLOAD].toString()
                        )
                        if (jsonData.has(ID)) {
                            val id = jsonData[ID].toString()
                            cassandraConnector.createKeysTableIfNotExists()
                            if (!cassandraConnector.isAlternateKeyExists(collection, id)) {
                                cassandraConnector.setDefaultKey(collection, id, uuid.toString())
                                call.respond(HttpStatusCode.OK, uuid.toString())
                            } else {
                                call.respond(HttpStatusCode.Conflict, "Object with id $id already exists")
                            }
                        } else {
                            call.respond(HttpStatusCode.OK, uuid.toString())
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
                    if (jsonData.length() > 0 && jsonData.has(ID) && jsonData.has(COLLECTION) && jsonData.has(PAYLOAD)) {
                        var id: String? = jsonData[ID].toString()
                        val collection = jsonData[COLLECTION].toString().toLowerCase()
                        if (cassandraConnector.isCollectionExists(collection)) {
                            id = cassandraConnector.getCorrectId(collection, id)
                            if (cassandraConnector.isIdExistsInCollection(collection, cassandraConnector.getCorrectId(collection, id))) {
                                cassandraConnector.updateRecordInCollection(
                                    collection,
                                    id.toString(),
                                    jsonData[PAYLOAD].toString()
                                )
                                call.respond(HttpStatusCode.OK)
                            } else {
                                call.respond(HttpStatusCode.NotFound, "Id $id in collection $collection not found")
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

                delete("/delete") {
                    if (call.parameters.contains(ID) && call.parameters.contains(COLLECTION)) {
                        val collection = call.parameters[COLLECTION].toString().toLowerCase()
                        val id = call.parameters[ID].toString()
                        if (cassandraConnector.isCollectionExists(collection)) {
                            if (cassandraConnector.isValidUUID(id))
                                if (cassandraConnector.isIdExistsInCollection(collection, id)) {
                                    cassandraConnector.deleteFromCollection(collection, id)
                                    call.respond(HttpStatusCode.OK)
                                } else {
                                    call.respond(HttpStatusCode.NotFound, "Id $id in collection $collection not found")
                                }
                        } else {
                            call.respond(HttpStatusCode.NotFound, "Collection $collection not found")
                        }
                    } else {
                        call.respond(
                            HttpStatusCode.BadRequest,
                            "Request should contain collection and id fields"
                        )
                    }
                }

                delete("/dropCollection") {
                    if (call.parameters.contains(COLLECTION)) {
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
                    if (call.parameters.contains(COLLECTION)) {
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