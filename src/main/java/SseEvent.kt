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
import io.ktor.http.*
import io.ktor.response.*
import kotlinx.coroutines.delay

enum class EventType {
    MESSAGE, EVENT, CLOSE, ERROR, KEEP_ALIVE;

    override fun toString(): String {
        return super.toString().toLowerCase()
    }
}

data class SseEvent(val data: String, val event: EventType? = null, val id: String? = null)

fun sseEventsFromList(list: List<String>): List<SseEvent> {
    val sseEventList: MutableList<SseEvent> = mutableListOf()
    var id = 0
    list.forEach {
        val event = SseEvent(it, EventType.MESSAGE, id.toString())
        sseEventList.add(event)
        id++
    }
    return  sseEventList
}

suspend fun ApplicationCall.respondSse(events: List<SseEvent>) {
    response.cacheControl(CacheControl.NoCache(null))
    respondTextWriter(contentType = ContentType.Text.EventStream) {
        for (event in events) {
            if (event.id != null) {
                write("id: ${event.id}\n")
            }
            if (event.event != null) {
                write("event: ${event.event}\n")
            }
            for (dataLine in event.data.lines()) {
                write("data: $dataLine\n")
            }
            write("\n")
            delay(1000)
            flush()
        }

        println("close")
        write("id: ${events.last().id?.toInt()?.plus(1)}\n")
        write("event: close\n")
        write("data: empty data\n")
        flush()
        close()
    }
}