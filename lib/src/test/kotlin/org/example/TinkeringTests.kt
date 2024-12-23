package org.example

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.schema
import kotlinx.serialization.*
import kotlinx.serialization.modules.SerializersModule
import kotlinx.serialization.modules.polymorphic
import kotlinx.serialization.modules.subclass
import org.apache.avro.SchemaNormalization
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

@OptIn(ExperimentalSerializationApi::class)
class TinkeringTests {

    @Test
    fun itShouldGenerateSchemas() {
        val schema = Avro.schema(Pizza.serializer())
        println(schema.toString(true))
    }

    @Test
    fun itShouldGeneratePolymorphicSchemas() {
        val avro = Avro {
            this.validateSerialization = true
        }

        val (module, serializer) = accountEventSerializerAndModule()

        val schema = avro.schema(serializer)

        println(schema.toString(true))

    }

    @Test
    fun itShouldSerializeAndDeserialize() {
        val avro = Avro {
            this.validateSerialization = true
        }
        val (module, serializer) = accountEventSerializerAndModule()
        val writerSchema = avro.schema(serializer)
        val fingerprint = SchemaNormalization.parsingFingerprint64(writerSchema)

        val initialEvent = AccountEvent.Opened(100u)
        val serializedEvent = avro.encodeToByteArray(writerSchema, serializer, initialEvent)

        val roundTripEvent = avro.decodeFromByteArray(writerSchema, serializer, serializedEvent)

        assertEquals(roundTripEvent, initialEvent)
    }

    fun accountEventSerializerAndModule() : Pair<SerializersModule, KSerializer<AccountEvent>> {

        val serializersModule = SerializersModule {
            polymorphic(AccountEvent::class) {
                subclass(AccountEvent.Opened::class)
                subclass(AccountEvent.Credited::class)
                subclass(AccountEvent.Debited::class)
            }
        }

        val serializer: KSerializer<AccountEvent> = serializersModule.serializer<AccountEvent>()
        return Pair(serializersModule, serializer)
    }
}