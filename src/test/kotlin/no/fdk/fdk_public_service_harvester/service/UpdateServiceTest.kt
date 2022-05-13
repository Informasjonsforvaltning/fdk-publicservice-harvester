package no.fdk.fdk_public_service_harvester.service

import com.nhaarman.mockitokotlin2.*
import no.fdk.fdk_public_service_harvester.configuration.ApplicationProperties
import no.fdk.fdk_public_service_harvester.repository.PublicServicesRepository
import no.fdk.fdk_public_service_harvester.utils.*
import org.apache.jena.rdf.model.Model
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test

@Tag("unit")
class UpdateServiceTest {
    private val metaRepository: PublicServicesRepository = mock()
    private val valuesMock: ApplicationProperties = mock()
    private val turtleService: TurtleService = mock()
    private val updateService = UpdateService(valuesMock, metaRepository, turtleService)

    private val responseReader = TestResponseReader()

    @Nested
    internal inner class UpdateMetaData {

        @Test
        fun catalogRecordsIsRecreatedFromMetaDBO() {
            whenever(metaRepository.findAll())
                .thenReturn(listOf(SERVICE_META_0, SERVICE_META_1))
            whenever(turtleService.getPublicService(SERVICE_ID_0, false))
                .thenReturn(responseReader.readFile("no_meta_service_0.ttl"))
            whenever(turtleService.getPublicService(SERVICE_ID_1, false))
                .thenReturn(responseReader.readFile("no_meta_service_1.ttl"))
            whenever(valuesMock.publicServiceHarvesterUri)
                .thenReturn("http://localhost:5000/public-services")

            updateService.updateMetaData()

            val expectedInfoModel0 = responseReader.parseFile("service_0.ttl", "TURTLE")
            val expectedInfoModel1 = responseReader.parseFile("service_1.ttl", "TURTLE")

            argumentCaptor<Model, String, Boolean>().apply {
                verify(turtleService, times(2)).saveAsPublicService(first.capture(), second.capture(), third.capture())
                assertTrue(checkIfIsomorphicAndPrintDiff(first.firstValue, expectedInfoModel0, "diffInMetaDataUpdatesTurtle-0"))
                assertTrue(checkIfIsomorphicAndPrintDiff(first.secondValue, expectedInfoModel1, "diffInMetaDataUpdatesTurtle-1"))
                assertEquals(listOf(SERVICE_ID_0, SERVICE_ID_1), second.allValues)
                assertEquals(listOf(true, true), third.allValues)
            }
        }

    }

    @Nested
    internal inner class UpdateUnionModel {

        @Test
        fun updateUnionModel() {
            whenever(metaRepository.findAll())
                .thenReturn(listOf(SERVICE_META_0, SERVICE_META_1, SERVICE_META_2, SERVICE_META_3))

            whenever(turtleService.getPublicService(SERVICE_ID_0, true))
                .thenReturn(responseReader.readFile("service_0.ttl"))
            whenever(turtleService.getPublicService(SERVICE_ID_1, true))
                .thenReturn(responseReader.readFile("service_1.ttl"))
            whenever(turtleService.getPublicService(SERVICE_ID_2, true))
                .thenReturn(responseReader.readFile("service_2.ttl"))
            whenever(turtleService.getPublicService(SERVICE_ID_3, true))
                .thenReturn(responseReader.readFile("service_3.ttl"))

            whenever(turtleService.getPublicService(SERVICE_ID_0, false))
                .thenReturn(responseReader.readFile("no_meta_service_0.ttl"))
            whenever(turtleService.getPublicService(SERVICE_ID_1, false))
                .thenReturn(responseReader.readFile("no_meta_service_1.ttl"))
            whenever(turtleService.getPublicService(SERVICE_ID_2, false))
                .thenReturn(responseReader.readFile("no_meta_service_2.ttl"))
            whenever(turtleService.getPublicService(SERVICE_ID_3, false))
                .thenReturn(responseReader.readFile("no_meta_service_3.ttl"))

            updateService.updateUnionModel()

            val expected = responseReader.parseFile("all_services.ttl", "TURTLE")
            val expectedNoRecords = responseReader.parseFile("no_meta_all_services.ttl", "TURTLE")

            argumentCaptor<Model, Boolean>().apply {
                verify(turtleService, times(2)).saveAsUnion(first.capture(), second.capture())
                assertTrue(checkIfIsomorphicAndPrintDiff(first.firstValue, expected, "updateUnionModel-withrecords"))
                assertTrue(checkIfIsomorphicAndPrintDiff(first.secondValue, expectedNoRecords, "updateUnionModel-norecords"))
                assertEquals(listOf(true, false), second.allValues)
            }
        }
    }
}