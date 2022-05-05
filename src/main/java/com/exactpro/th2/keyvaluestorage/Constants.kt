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

const val COLLECTION = "collection"
const val ID = "id"
const val PAYLOAD = "payload"
const val WORKSPACE_LINKS = "workspace_links"
const val TIMESTAMP = "timestamp"
const val SORT = "sort"
const val ASCENDING = "ascending"
const val ALTERNATE_KEYS_STORAGE = "alternate_keys_storage"
const val USER_PARTITION_KEY_STORAGE = "user_partition_key_storage"
const val NAME = "name"
const val USER = "user"
const val CRADLE_CONFIDENTIAL_FILE_NAME = "cradle.json"
const val CUSTOM_JSON_FILE = "custom.json"
const val USER_ID = "userId"
const val DB_RETRY_DELAY: Long = 3000
const val PREFERENCES = "preferences"
const val DEFAULT_USER_PREFERENCES:String = "{messageDisplayRules: {rootRule: {" +
        "editableSession: false," +
        "editableType: true," +
        "id: \'root\'," +
        "removable: false," +
        "session: \'*\'," +
        "viewType: \'json\'," +
        "}," +
        "rules: []," +
        "}," +
        "messageBodySortOrder: []," +
        "pinned: {" +
        "events: []," +
        "messages: []," +
        "}," +
        "lastSearchedSessions: []," +
        "}"