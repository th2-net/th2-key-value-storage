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

package com.exactpro.th2.keyvaluestorage; import com.fasterxml.jackson.annotation.JsonProperty;
import org.jetbrains.annotations.NotNull;

import java.math.BigInteger;
import java.util.Comparator;

public class Record implements Comparable<Record>{
    @JsonProperty("id")
    String id;
    @JsonProperty("json")
    String json;
    @JsonProperty("time")
    BigInteger time;

    public Record(String id, String json, BigInteger time) {
        this.id = id;
        this.json = json.replaceAll("\"", "");
        this.time = time;
    }

    public void setId(String id){
        this.id = id;
    }

    public void setJson(String json){
        this.json = json.replaceAll("\"", "");
    }

    public void setTime(BigInteger time){
        this.time = time;
    }

    @Override
    public int compareTo(@NotNull Record record) {
        return Comparators.TIME.compare(this, record);
    }

    public static class Comparators {

        public static Comparator<Record> TIME = new Comparator<Record>() {
            @Override
            public int compare(Record o1, Record o2) {
                return o1.time.compareTo(o2.time);
            }
        };
    }
}
