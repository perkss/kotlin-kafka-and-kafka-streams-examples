package com.perkss.kafka.reactive.model

import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord

data class Customer(
    val id: String,
    val name: String,
    val city: String
)

fun Customer.toGenericRecord(schema: Schema): GenericRecord {
    val record = GenericData.Record(schema)
    record.put("id", id)
    record.put("name", name)
    record.put("city", city)
    return record
}

fun GenericRecord.toCustomer(): Customer {
    return Customer(
        get("id") as String,
        get("name") as String,
        get("city") as String
    )
}

// TODO pass file
object SchemaLoader {
    fun loadSchema() = Schema.Parser()
        .parse(
            "{\n" +
                    "  \"namespace\": \"com.perkss\",\n" +
                    "  \"type\": \"record\",\n" +
                    "  \"name\": \"Customer\",\n" +
                    "  \"fields\": [\n" +
                    "    {\n" +
                    "      \"name\": \"id\",\n" +
                    "      \"type\": {\n" +
                    "        \"type\": \"string\",\n" +
                    "        \"avro.java.string\": \"String\"\n" +
                    "      }\n" +
                    "    },\n" +
                    "    {\n" +
                    "      \"name\": \"name\",\n" +
                    "      \"type\": {\n" +
                    "        \"type\": \"string\",\n" +
                    "        \"avro.java.string\": \"String\"\n" +
                    "      }\n" +
                    "    },\n" +
                    "    {\n" +
                    "      \"name\": \"city\",\n" +
                    "      \"type\": {\n" +
                    "        \"type\": \"string\",\n" +
                    "        \"avro.java.string\": \"String\"\n" +
                    "      }\n" +
                    "    }\n" +
                    "  ]\n" +
                    "}"
        )
}