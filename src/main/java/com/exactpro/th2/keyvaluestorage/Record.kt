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

import com.fasterxml.jackson.annotation.JsonProperty
import java.math.BigInteger

open class Record(id: String, json: String, time: BigInteger) : Comparable<Record> {
    @JsonProperty("id")
    var id: String = id

    @JsonProperty("json")
    var json: String = json.replace("\"", "")

    @JsonProperty("time")
    var time: BigInteger = time

    override operator fun compareTo(other: Record): Int {
        return Comparators.TIME.compare(this, other)
    }

    object Comparators {
        var TIME =
            java.util.Comparator<Record> { o1, o2 -> o1.time.compareTo(o2.time) }
    }
}
