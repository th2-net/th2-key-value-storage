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

    private val COLLECTION: String = "collection"
    private val ID: String = "id"
    private val PAYLOAD: String = "payload"
    private val WORKSPACE_LINKS: String = "workspace_links"

    fun run() {
        embeddedServer(Netty, port = 8080) {
            install(ContentNegotiation) {
                gson {
                    setDateFormat(DateFormat.LONG)
                    setPrettyPrinting()
                }
            }
            install(CORS) {
                anyHost()
            }

            routing {
                val cassandraConnector = CassandraConnector()
                cassandraConnector.connect()

                get("/idsFromCollection") {
                    val parameters = call.request.queryParameters
                    if (parameters.contains(COLLECTION)) {
                        val collection = parameters[COLLECTION]?.toLowerCase()
                        if (cassandraConnector.isCollectionExists(collection)) {
                            val ids = cassandraConnector.getIdsFromCollection(collection)
                            ids.reverse()
                            call.respondSse(sseEventsFromList(ids))
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
                            records.reverse()
                            call.respondSse(sseEventsFromList(records))
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
                            val key = cassandraConnector.getCorrectId(collection, id);
                            if (key != null) {
                                if (cassandraConnector.isIdExistsInCollection(collection, key)) {
                                    val result = cassandraConnector.getByIdFromCollection(collection, key)
                                    call.respond(HttpStatusCode.OK, result)
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

                get("/delete") {
                    call.respond(HttpStatusCode.OK, "Delete")
                }

                get("/getWorkspaceLink") {
                    if (cassandraConnector.isCollectionExists(WORKSPACE_LINKS)) {
                        if (call.parameters.contains(ID)) {
                            val id = call.parameters[ID]
                            if (cassandraConnector.isIdExistsInCollection(WORKSPACE_LINKS, id)) {
                                val result = cassandraConnector.getByIdFromCollection(WORKSPACE_LINKS, id)
                                call.respond(HttpStatusCode.OK, result)
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
                        val uuid = cassandraConnector.insertIntoTable(
                            jsonData[COLLECTION].toString(),
                            jsonData[PAYLOAD].toString()
                        )
                        call.respond(HttpStatusCode.OK, uuid)
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
                        val id = jsonData[ID].toString()
                        val collection = jsonData[COLLECTION].toString()
                        if (cassandraConnector.isCollectionExists(collection)) {
                            if (cassandraConnector.isIdExistsInCollection(collection, id)) {
                                cassandraConnector.updateRecordInTable(
                                    collection,
                                    id,
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
            }
        }.start(wait = true)
    }
}