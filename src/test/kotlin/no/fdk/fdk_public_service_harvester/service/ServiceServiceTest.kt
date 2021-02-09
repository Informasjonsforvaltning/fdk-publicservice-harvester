package no.fdk.fdk_public_service_harvester.service

import com.nhaarman.mockitokotlin2.mock
import com.nhaarman.mockitokotlin2.whenever
import no.fdk.fdk_public_service_harvester.rdf.JenaType
import no.fdk.fdk_public_service_harvester.utils.*
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
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
            whenever(turtleService.getUnion())
                .thenReturn(null)

            val expected = responseReader.parseResponse("", "TURTLE")

            val responseTurtle = service.getAll(JenaType.TURTLE)
            val responseJsonLD = service.getAll(JenaType.JSON_LD)

            assertTrue(expected.isIsomorphicWith(responseReader.parseResponse(responseTurtle, "TURTLE")))
            assertTrue(expected.isIsomorphicWith(responseReader.parseResponse(responseJsonLD, "JSON-LD")))
        }

        @Test
        fun getAllHandlesTurtleAndOtherRDF() {
            val allCatalogs = javaClass.classLoader.getResource("all_services.ttl")!!.readText()
            whenever(turtleService.getUnion())
                .thenReturn(allCatalogs)

            val expected = responseReader.parseFile("all_services.ttl", "TURTLE")

            val responseTurtle = service.getAll(JenaType.TURTLE)
            val responseN3 = service.getAll(JenaType.N3)
            val responseNTriples = service.getAll(JenaType.NTRIPLES)

            assertTrue(expected.isIsomorphicWith(responseReader.parseResponse(responseTurtle, "TURTLE")))
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

            val response = service.getServiceById("123", JenaType.TURTLE)

            assertNull(response)
        }

        @Test
        fun responseIsIsomorphicWithExpectedModel() {
            whenever(turtleService.getPublicService(SERVICE_ID_0, true))
                .thenReturn(javaClass.classLoader.getResource("service_0.ttl")!!.readText())

            val responseTurtle = service.getServiceById(SERVICE_ID_0, JenaType.TURTLE)
            val responseRDFXML = service.getServiceById(SERVICE_ID_0, JenaType.RDF_XML)

            val expected = responseReader.parseFile("service_0.ttl", "TURTLE")

            assertTrue(expected.isIsomorphicWith(responseReader.parseResponse(responseTurtle!!, "TURTLE")))
            assertTrue(expected.isIsomorphicWith(responseReader.parseResponse(responseRDFXML!!, "RDF/XML")))
        }

    }

}