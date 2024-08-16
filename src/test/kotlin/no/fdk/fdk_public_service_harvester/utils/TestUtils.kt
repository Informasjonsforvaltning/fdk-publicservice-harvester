package no.fdk.fdk_public_service_harvester.utils

import com.mongodb.ConnectionString
import com.mongodb.MongoClientSettings
import com.mongodb.client.MongoClient
import com.mongodb.client.MongoClients
import no.fdk.fdk_public_service_harvester.rdf.createRDFResponse
import no.fdk.fdk_public_service_harvester.utils.ApiTestContext.Companion.mongoContainer
import org.apache.jena.rdf.model.Model
import org.apache.jena.riot.Lang
import org.bson.codecs.configuration.CodecRegistries
import org.bson.codecs.pojo.PojoCodecProvider
import org.slf4j.LoggerFactory
import org.springframework.http.HttpEntity
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpMethod
import java.io.BufferedReader
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory
import org.springframework.web.client.HttpClientErrorException
import org.springframework.web.client.RestTemplate
import java.net.HttpURLConnection
import java.net.URI

private val logger = LoggerFactory.getLogger(ApiTestContext::class.java)

fun apiGet(endpoint: String, acceptHeader: String?, port: Int): Map<String, Any> {

    return try {
        val connection = URI("http://localhost:$port$endpoint").toURL().openConnection() as HttpURLConnection
        if (acceptHeader != null) connection.setRequestProperty("Accept", acceptHeader)
        connection.connect()

        if (isOK(connection.responseCode)) {
            val responseBody = connection.inputStream.bufferedReader().use(BufferedReader::readText)
            mapOf(
                "body" to responseBody,
                "header" to connection.headerFields.toString(),
                "status" to connection.responseCode
            )
        } else {
            mapOf(
                "status" to connection.responseCode,
                "header" to " ",
                "body" to " "
            )
        }
    } catch (e: Exception) {
        mapOf(
            "status" to e.toString(),
            "header" to " ",
            "body" to " "
        )
    }
}

fun authorizedRequest(
    path: String,
    token: String?,
    port: Int,
    method: HttpMethod = HttpMethod.POST,
    body: String? = null
): Map<String, Any> {
    val request = RestTemplate()
    request.requestFactory = HttpComponentsClientHttpRequestFactory()
    val url = "http://localhost:$port$path"
    val httpHeaders = HttpHeaders()
    token?.let { httpHeaders.setBearerAuth(it) }
    httpHeaders.contentType = MediaType.APPLICATION_JSON
    val entity: HttpEntity<String> = HttpEntity(body, httpHeaders)

    return try {
        val response = request.exchange(url, method, entity, String::class.java)
        mapOf(
            "body" to response.body,
            "header" to response.headers.toString(),
            "status" to response.statusCode.value()
        )
    } catch (e: HttpClientErrorException) {
        mapOf(
            "status" to e.statusCode.value(),
            "header" to " ",
            "body" to e.toString()
        )
    } catch (e: Exception) {
        mapOf(
            "status" to e.toString(),
            "header" to " ",
            "body" to " "
        )
    }
}

private fun isOK(response: Int?): Boolean =
    if (response == null) false
    else HttpStatus.resolve(response)?.is2xxSuccessful == true

fun populateDB() {
    val connectionString =
        ConnectionString("mongodb://${MONGO_USER}:${MONGO_PASSWORD}@localhost:${mongoContainer.getMappedPort(MONGO_PORT)}/$MONGO_COLLECTION?authSource=admin&authMechanism=SCRAM-SHA-1")
    val pojoCodecRegistry = CodecRegistries.fromRegistries(
        MongoClientSettings.getDefaultCodecRegistry(),
        CodecRegistries.fromProviders(PojoCodecProvider.builder().automatic(true).build())
    )

    val client: MongoClient = MongoClients.create(connectionString)
    val mongoDatabase = client.getDatabase(MONGO_COLLECTION).withCodecRegistry(pojoCodecRegistry)

    val sourceCollection = mongoDatabase.getCollection("harvestSourceTurtle")
    sourceCollection.insertMany(sourceTurtleDBPopulation())

    val serviceTurtleCollection = mongoDatabase.getCollection("serviceTurtle")
    serviceTurtleCollection.insertMany(serviceTurtleDBPopulation())

    val fdkServiceTurtleCollection = mongoDatabase.getCollection("fdkServiceTurtle")
    fdkServiceTurtleCollection.insertMany(fdkTurtleDBPopulation())

    val catalogTurtleCollection = mongoDatabase.getCollection("catalogTurtle")
    catalogTurtleCollection.insertMany(catalogTurtleDBPopulation())

    val fdkCatalogTurtleCollection = mongoDatabase.getCollection("fdkCatalogTurtle")
    fdkCatalogTurtleCollection.insertMany(fdkCatalogTurtleDBPopulation())

    val serviceMetaCollection = mongoDatabase.getCollection("serviceMeta")
    serviceMetaCollection.insertMany(metaDBPopulation())

    val catalogMetaCollection = mongoDatabase.getCollection("catalogMeta")
    catalogMetaCollection.insertMany(metaCatalogPopulation())

    client.close()
}

fun checkIfIsomorphicAndPrintDiff(actual: Model, expected: Model, name: String): Boolean {
    val isIsomorphic = actual.isIsomorphicWith(expected)

    if (!isIsomorphic) {
        val actualDiff = actual.difference(expected).createRDFResponse(Lang.TURTLE)
        val expectedDiff = expected.difference(actual).createRDFResponse(Lang.TURTLE)

        if (actualDiff.isNotEmpty()) {
            logger.error("non expected nodes in $name:")
            logger.error(actualDiff)
        }
        if (expectedDiff.isNotEmpty()) {
            logger.error("missing nodes in $name:")
            logger.error(expectedDiff)
        }
    }
    return isIsomorphic
}
