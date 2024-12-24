package org.example

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.decodeFromGenericData
import com.github.avrokotlin.avro4k.encodeToGenericData
import com.github.avrokotlin.avro4k.schema
import kotlinx.serialization.*
import kotlinx.serialization.modules.SerializersModule
import kotlinx.serialization.modules.plus
import kotlinx.serialization.modules.polymorphic
import kotlinx.serialization.modules.subclass
import org.apache.avro.Schema
import org.apache.avro.SchemaCompatibility
import org.apache.avro.SchemaNormalization
import org.apache.avro.generic.GenericDatumReader
import org.apache.avro.generic.GenericDatumWriter
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.DecoderFactory
import org.apache.avro.io.EncoderFactory
import org.apache.commons.io.output.ByteArrayOutputStream
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.util.Base64
import kotlin.reflect.typeOf

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
        // https://avro.apache.org/docs/1.7.5/api/java/org/apache/avro/SchemaNormalization.html
        val fingerprintSha256 = SchemaNormalization.parsingFingerprint("SHA-256", writerSchema)
        val b64FingerprintSha256 = Base64.getEncoder().encodeToString(fingerprintSha256)
        val fingerprint64 = SchemaNormalization.parsingFingerprint64(writerSchema)
        println(b64FingerprintSha256)
        println(fingerprint64)

        val initialEvent = AccountEvent.Opened(100u)

        val genericInitialEvent = avro.encodeToGenericData(writerSchema, initialEvent)

        // serializer
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

    @Test
    fun defaultAndExplicitlyDefinedSerializersModuleShouldProduceSameSchema() {
        val avro = Avro {
            this.validateSerialization = true
        }
        val (module, serializerWithExplicitPoly) = accountEventSerializerAndModule()
        val explicitPolySchema = avro.schema(serializerWithExplicitPoly)
        val defaultSchema = avro.schema<AccountEvent>()
        val defaultSerializer = AccountEvent.serializer()

        assertEquals(defaultSchema, explicitPolySchema)

    }

    @Test
    fun iCanUseAvroSerializationOnceIHaveTheSchema() {

        val avro = Avro {  }

        val accountEventSchema = avro.schema<AccountEvent>()

        val initialEvent = AccountEvent.Opened(100u)

        val genericData = avro.encodeToGenericData(accountEventSchema, initialEvent)

        val decoded = avro.decodeFromGenericData<AccountEvent>(accountEventSchema, genericData)

        assertEquals(decoded, initialEvent)
    }

    @Test
    fun itShouldCalculateParsingForm() {
        val parser = Schema.Parser()
        val userSchema1 = parser.parse(exampleUserSchemaString1)
        val parsingForm = SchemaNormalization.toParsingForm(userSchema1)
        println(parsingForm)
    }

    @Test
    fun itShouldSupportSchemaCompatabilityChecks() {
//        val parser = Schema.Parser()
        val userSchema1 = Schema.Parser().parse(exampleUserSchemaString1)
        val userSchema2 = Schema.Parser().parse(exampleUserSchemaString2)

        val readerSchema = userSchema2
        val writerSchema = userSchema1

        val compatabilityResult = SchemaCompatibility.checkReaderWriterCompatibility(readerSchema, writerSchema)
        when (compatabilityResult.result.compatibility) {
            SchemaCompatibility.SchemaCompatibilityType.COMPATIBLE -> {

            }
            SchemaCompatibility.SchemaCompatibilityType.INCOMPATIBLE -> {
                Assertions.fail("not expecting schemas to be incompatible")
            }
            SchemaCompatibility.SchemaCompatibilityType.RECURSION_IN_PROGRESS -> {
                Assertions.fail("not expecting to be recursion in progress")
            }
            null -> {
                Assertions.fail("not expecting to be null")
            }
        }
    }

    @Test
    fun itShouldHaveInteropWithAvroSerialization() {

        val avro = Avro {  }

        val schema = avro.schema<AccountEvent>()
        val serializer = AccountEvent.serializer()

        val genericWriter = GenericDatumWriter<GenericRecord>(schema)
        val genericReader = GenericDatumReader<GenericRecord>(schema)

        val initialEvent = AccountEvent.Opened(100u)

        // serialize to byte array using avro4k
        val binaryFromAvro4k = avro.encodeToByteArray(schema, serializer, initialEvent)

        // deserialize to generic data
        val genericDataFromBinary = DecoderFactory.get().binaryDecoder(binaryFromAvro4k, null).let { decoder ->
            genericReader.read(null, decoder)
        }

        // serialize with json encoder
        val json = ByteArrayOutputStream().let { baos ->
            val encoder = EncoderFactory.get().jsonEncoder(schema, baos)
            genericWriter.write(genericDataFromBinary, encoder)
            encoder.flush()
            baos.close()
            baos.toString(Charsets.UTF_8)
        }

        // deserialize with json decoder
        val genericDataFromJson = DecoderFactory.get().jsonDecoder(schema, json).let { decoder ->
            genericReader.read(null, decoder)

        }
        assertEquals(genericDataFromJson, genericDataFromBinary)

        // serialize back to byte array
        val binaryFromGenericData = ByteArrayOutputStream().let { baos ->
            val encoder = EncoderFactory.get().directBinaryEncoder(baos, null)
            genericWriter.write(genericDataFromJson, encoder)
            encoder.flush()
            baos.close()
            baos.toByteArray()
        }

//        assertEquals(binaryFromGenericData, binaryFromAvro4k)
        // deserialize from byte array with avro4k
        val roundTripEvent = avro.decodeFromByteArray(schema, serializer, binaryFromGenericData)

        assertEquals(roundTripEvent, initialEvent)



    }


    val exampleUserSchemaString1 =  """
        {"namespace": "example.avro",
         "type": "record",
         "name": "User",
         "fields": [
             {"name": "name", "type": "string"},
             {"name": "favorite_number",  "type": ["int", "null"]},
             {"name": "favorite_color", "type": ["string", "null"]}
         ]
        }

    """.trimIndent()

    val exampleUserSchemaString2 =  """
        {"namespace": "example.avro",
         "type": "record",
         "name": "User",
         "fields": [
             {"name": "name", "type": "string"},
             {"name": "favorite_number",  "type": ["int", "null"]},
             {"name": "favorite_color", "type": ["string", "null"]},
             {"name": "favorite_state", "type": ["string", "null"], "default": "null" }
         ]
        }

    """.trimIndent()

    @OptIn(InternalSerializationApi::class)
    fun accountEventSerializerAndModule() : Pair<SerializersModule, KSerializer<AccountEvent>> {

        val serializersModule = SerializersModule {
            polymorphic(AccountEvent::class) {
                subclass(AccountEvent.Opened::class)
                subclass(AccountEvent.Credited::class)
                subclass(AccountEvent.Debited::class)
            }
        } + SerializersModule { /* just testing out duplicating the schema here */
            polymorphic(AccountEvent::class) {
                subclass(AccountEvent.Opened::class)
                subclass(AccountEvent.Credited::class)
                subclass(AccountEvent.Debited::class)
            }
        }

        typeOf<AccountEvent>()
        val serializer: KSerializer<AccountEvent> = serializersModule.serializer<AccountEvent>()
        return Pair(serializersModule, serializer)
    }
}