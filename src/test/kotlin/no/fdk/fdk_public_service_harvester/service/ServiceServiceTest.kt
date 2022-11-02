package no.fdk.fdk_public_service_harvester.service

import no.fdk.fdk_public_service_harvester.utils.*
import org.apache.jena.riot.Lang
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever
import kotlin.test.assertNull
import kotlin.test.assertTrue

@Tag("unit")
class PublicServicesServiceTest {
    private val turtleService: TurtleService = mock()
    private val service = PublicServicesService(turtleService)

    private val responseReader = TestResponseReader()

    @Nested
    internal inner class AllServices {

        @Test
        fun responseIsometricWithEmptyModelForEmptyDB() {
            whenever(turtleService.getUnion(true))
                .thenReturn(null)

            val expected = responseReader.parseResponse("", "TURTLE")

            val responseTurtle = service.getAll(Lang.TURTLE, true)
            val responseJsonLD = service.getAll(Lang.JSONLD, true)

            assertTrue(expected.isIsomorphicWith(responseReader.parseResponse(responseTurtle, "TURTLE")))
            assertTrue(expected.isIsomorphicWith(responseReader.parseResponse(responseJsonLD, "JSON-LD")))
        }

        @Test
        fun getAllHandlesTurtleAndOtherRDF() {
            whenever(turtleService.getUnion(true))
                .thenReturn(javaClass.classLoader.getResource("all_services.ttl")!!.readText())
            whenever(turtleService.getUnion(false))
                .thenReturn(javaClass.classLoader.getResource("no_meta_all_services.ttl")!!.readText())

            val expected = responseReader.parseFile("all_services.ttl", "TURTLE")
            val expectedNoRecords = responseReader.parseFile("no_meta_all_services.ttl", "TURTLE")

            val responseTurtle = service.getAll(Lang.TURTLE, false)
            val responseN3 = service.getAll(Lang.N3, true)
            val responseNTriples = service.getAll(Lang.NTRIPLES, true)

            assertTrue(expectedNoRecords.isIsomorphicWith(responseReader.parseResponse(responseTurtle, "TURTLE")))
            assertTrue(expected.isIsomorphicWith(responseReader.parseResponse(responseN3, "N3")))
            assertTrue(expected.isIsomorphicWith(responseReader.parseResponse(responseNTriples, "N-TRIPLES")))
        }

    }

    @Nested
    internal inner class ServiceById {

        @Test
        fun responseIsNullWhenNoModelIsFound() {
            whenever(turtleService.getPublicService("123", true))
                .thenReturn(null)

            val response = service.getServiceById("123", Lang.TURTLE, true)

            assertNull(response)
        }

        @Test
        fun responseIsIsomorphicWithExpectedModel() {
            whenever(turtleService.getPublicService(SERVICE_ID_0, true))
                .thenReturn(javaClass.classLoader.getResource("service_0.ttl")!!.readText())
            whenever(turtleService.getPublicService(SERVICE_ID_0, false))
                .thenReturn(javaClass.classLoader.getResource("no_meta_service_0.ttl")!!.readText())

            val responseTurtle = service.getServiceById(SERVICE_ID_0, Lang.TURTLE, true)
            val responseRDFXML = service.getServiceById(SERVICE_ID_0, Lang.RDFXML, false)

            val expected = responseReader.parseFile("service_0.ttl", "TURTLE")
            val expectedNoRecords = responseReader.parseFile("no_meta_service_0.ttl", "TURTLE")

            assertTrue(expected.isIsomorphicWith(responseReader.parseResponse(responseTurtle!!, "TURTLE")))
            assertTrue(expectedNoRecords.isIsomorphicWith(responseReader.parseResponse(responseRDFXML!!, "RDF/XML")))
        }

    }

}
