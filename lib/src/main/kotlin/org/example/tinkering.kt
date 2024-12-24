package org.example

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.encodeToGenericData
import com.github.avrokotlin.avro4k.encodeToStream
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.KSerializer
import kotlinx.serialization.Serializable
import org.apache.avro.Schema
import org.apache.avro.generic.GenericDatumReader
import org.apache.avro.generic.GenericDatumWriter
import org.apache.avro.io.EncoderFactory
import org.apache.commons.io.output.ByteArrayOutputStream


@Serializable
data class Ingredient(val name: String, val sugar: Double)

@Serializable
data class Pizza(val name: String, val ingredients: List<Ingredient>, val topping: Ingredient?, val vegetarian: Boolean)




@Serializable
sealed interface AccountEvent {

    @Serializable
    data class Opened(val initialDeposit: ULong) : AccountEvent
    @Serializable
    data class Credited(val amount: ULong) : AccountEvent
    @Serializable
    data class Debited(val amount: ULong) : AccountEvent

}

interface Serializer<T : Any> {
    fun serialize(record: T) : String
    fun deserialize(value: String) : T
}

open class AvroJsonSerializer<T : Any>(
    private val avro: Avro,
    private val schema: Schema,
    private val serializer: KSerializer<T>,
) : Serializer<T> {

    val writer = GenericDatumWriter<T>(schema)
    val reader = GenericDatumReader<T>(schema)

    override fun serialize(record: T): String {
        val encoded = avro.encodeToByteArray(schema, serializer, record)


        TODO("Not yet implemented")
    }

    override fun deserialize(value: String): T {
        TODO("Not yet implemented")
    }

}