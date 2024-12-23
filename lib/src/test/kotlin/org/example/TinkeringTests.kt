package org.example

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.decodeFromGenericData
import com.github.avrokotlin.avro4k.encodeToGenericData
import com.github.avrokotlin.avro4k.schema
import kotlinx.serialization.*
import kotlinx.serialization.modules.SerializersModule
import kotlinx.serialization.modules.polymorphic
import kotlinx.serialization.modules.subclass
import org.apache.avro.SchemaNormalization
import org.apache.avro.generic.GenericDatumReader
import org.apache.avro.generic.GenericDatumWriter
import org.apache.avro.io.DecoderFactory
import org.apache.avro.io.EncoderFactory
import org.apache.commons.io.output.ByteArrayOutputStream
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

    @Test
    fun itShouldEncodeAndDecodeWithJson() {
        val avro = Avro {
            this.validateSerialization = true
        }
        val (module, serializer) = accountEventSerializerAndModule()
        val writerSchema = avro.schema(serializer)
        val fingerprint = SchemaNormalization.parsingFingerprint64(writerSchema)

        val initialEvent = AccountEvent.Opened(100u)

        val genericInitialEvent = avro.encodeToGenericData(writerSchema, initialEvent)

        val out = ByteArrayOutputStream()
        val writer = GenericDatumWriter<Any>(writerSchema)
        val encoder = EncoderFactory.get().jsonEncoder(writerSchema, out)
        writer.write(genericInitialEvent, encoder)
        encoder.flush()
        out.close()

        val jsonOutput = out.toString(Charsets.UTF_8)
        println(jsonOutput)


        val reader = GenericDatumReader<Any>(writerSchema)
        val decoder = DecoderFactory.get().jsonDecoder(writerSchema, jsonOutput)
        val genericRoundTripEvent = reader.read(null, decoder)

        val roundTripInitialEvent = avro.decodeFromGenericData(writerSchema, serializer, genericRoundTripEvent)

        assertEquals(roundTripInitialEvent, initialEvent)

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