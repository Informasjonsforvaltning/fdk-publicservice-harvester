package no.fdk.fdk_public_service_harvester.harvester

import com.nhaarman.mockitokotlin2.*
import no.fdk.fdk_public_service_harvester.adapter.ServicesAdapter
import no.fdk.fdk_public_service_harvester.configuration.ApplicationProperties
import no.fdk.fdk_public_service_harvester.model.FdkIdAndUri
import no.fdk.fdk_public_service_harvester.model.HarvestReport
import no.fdk.fdk_public_service_harvester.model.PublicServiceMeta
import no.fdk.fdk_public_service_harvester.repository.PublicServicesRepository
import no.fdk.fdk_public_service_harvester.service.TurtleService
import no.fdk.fdk_public_service_harvester.utils.*
import org.apache.jena.rdf.model.Model
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import java.util.*
import kotlin.test.assertEquals

@Tag("unit")
class HarvesterTest {
    private val metaRepository: PublicServicesRepository = mock()
    private val turtleService: TurtleService = mock()
    private val valuesMock: ApplicationProperties = mock()
    private val adapter: ServicesAdapter = mock()

    private val harvester = PublicServicesHarvester(adapter, metaRepository, turtleService, valuesMock)
    private val responseReader = TestResponseReader()

    @Test
    fun harvestDataSourceSavedWhenDBIsEmpty() {
        whenever(adapter.fetchServices(TEST_HARVEST_SOURCE))
            .thenReturn(responseReader.readFile("harvest_response_0.ttl"))
        whenever(valuesMock.publicServiceHarvesterUri)
            .thenReturn("http://localhost:5000/public-services")

        val report = harvester.harvestServices(TEST_HARVEST_SOURCE, TEST_HARVEST_DATE)

        argumentCaptor<Model, String>().apply {
            verify(turtleService, times(1)).saveAsHarvestSource(first.capture(), second.capture())
            assertTrue(first.firstValue.isIsomorphicWith(responseReader.parseFile("harvest_response_0.ttl", "TURTLE")))
            Assertions.assertEquals(TEST_HARVEST_SOURCE.url, second.firstValue)
        }

        argumentCaptor<Model, String, Boolean>().apply {
            verify(turtleService, times(6)).saveAsPublicService(first.capture(), second.capture(), third.capture())
            assertTrue(checkIfIsomorphicAndPrintDiff(first.allValues[0], responseReader.parseFile("no_meta_service_0.ttl", "TURTLE"), "harvestDataSourceSavedWhenDBIsEmpty-norecords0"))
            assertTrue(checkIfIsomorphicAndPrintDiff(first.allValues[1], responseReader.parseFile("service_0.ttl", "TURTLE"), "harvestDataSourceSavedWhenDBIsEmpty-0"))
            assertTrue(checkIfIsomorphicAndPrintDiff(first.allValues[2], responseReader.parseFile("no_meta_service_1.ttl", "TURTLE"), "harvestDataSourceSavedWhenDBIsEmpty-norecords1"))
            assertTrue(checkIfIsomorphicAndPrintDiff(first.allValues[3], responseReader.parseFile("service_1.ttl", "TURTLE"), "harvestDataSourceSavedWhenDBIsEmpty-1"))
            assertTrue(checkIfIsomorphicAndPrintDiff(first.allValues[4], responseReader.parseFile("no_meta_service_2.ttl", "TURTLE"), "harvestDataSourceSavedWhenDBIsEmpty-norecords2"))
            assertTrue(checkIfIsomorphicAndPrintDiff(first.allValues[5], responseReader.parseFile("service_2.ttl", "TURTLE"), "harvestDataSourceSavedWhenDBIsEmpty-2"))
            assertEquals(listOf(SERVICE_ID_0, SERVICE_ID_0, SERVICE_ID_1, SERVICE_ID_1, SERVICE_ID_2, SERVICE_ID_2), second.allValues)
            Assertions.assertEquals(listOf(false, true, false, true, false, true), third.allValues)
        }

        argumentCaptor<PublicServiceMeta>().apply {
            verify(metaRepository, times(3)).save(capture())
            assertEquals(listOf(SERVICE_META_0, SERVICE_META_1, SERVICE_META_2), allValues.sortedBy { it.uri })
        }

        val expectedReport = HarvestReport(
            id="test-source",
            url="http://localhost:5000/fdk-public-service-publisher.ttl",
            dataType="publicService",
            harvestError=false,
            startTime = "2020-10-05 15:15:39 +0200",
            endTime = report!!.endTime,
            changedResources = listOf(
                FdkIdAndUri(fdkId="d5d0c07c-c14f-3741-9aa3-126960958cf0", uri="http://public-service-publisher.fellesdatakatalog.digdir.no/services/1"),
                FdkIdAndUri(fdkId="6ce4e524-3226-3591-ad99-c026705d4259", uri="http://public-service-publisher.fellesdatakatalog.digdir.no/services/2"),
                FdkIdAndUri(fdkId="31249174-df02-3746-9d61-59fc61b4c5f9", uri="http://public-service-publisher.fellesdatakatalog.digdir.no/services/3"))
        )

        assertEquals(expectedReport, report)
    }

    @Test
    fun harvestDataSourceNotPersistedWhenNoChangesFromDB() {
        val harvested = responseReader.readFile("harvest_response_0.ttl")
        whenever(adapter.fetchServices(TEST_HARVEST_SOURCE))
            .thenReturn(harvested)
        whenever(valuesMock.publicServiceHarvesterUri)
            .thenReturn("http://localhost:5000/public-services")
        whenever(turtleService.getHarvestSource(TEST_HARVEST_SOURCE.url!!))
            .thenReturn(harvested)

        val report = harvester.harvestServices(TEST_HARVEST_SOURCE, TEST_HARVEST_DATE)

        argumentCaptor<Model, String>().apply {
            verify(turtleService, times(0)).saveAsHarvestSource(first.capture(), second.capture())
        }

        argumentCaptor<Model, String, Boolean>().apply {
            verify(turtleService, times(0)).saveAsPublicService(first.capture(), second.capture(), third.capture())
        }

        argumentCaptor<PublicServiceMeta>().apply {
            verify(metaRepository, times(0)).save(capture())
        }

        val expectedReport = HarvestReport(
            id="test-source",
            url="http://localhost:5000/fdk-public-service-publisher.ttl",
            dataType="publicService",
            harvestError=false,
            startTime = "2020-10-05 15:15:39 +0200",
            endTime = report!!.endTime
        )

        assertEquals(expectedReport, report)
    }

    @Test
    fun onlyRelevantUpdatedWhenHarvestedFromDB() {
        whenever(adapter.fetchServices(TEST_HARVEST_SOURCE))
            .thenReturn(responseReader.readFile("harvest_response_0.ttl"))
        whenever(valuesMock.publicServiceHarvesterUri)
            .thenReturn("http://localhost:5000/public-services")
        whenever(turtleService.getHarvestSource(TEST_HARVEST_SOURCE.url!!))
            .thenReturn(responseReader.readFile("harvest_response_0_diff.ttl"))
        whenever(metaRepository.findById(SERVICE_META_0.uri))
            .thenReturn(Optional.of(SERVICE_META_0))
        whenever(metaRepository.findById(SERVICE_META_1.uri))
            .thenReturn(Optional.of(SERVICE_META_1))
        whenever(metaRepository.findById(SERVICE_META_2.uri))
            .thenReturn(Optional.of(SERVICE_META_2))
        whenever(turtleService.getPublicService(SERVICE_ID_0, false))
            .thenReturn(responseReader.readFile("no_meta_service_0_diff.ttl"))
        whenever(turtleService.getPublicService(SERVICE_ID_1, false))
            .thenReturn(responseReader.readFile("no_meta_service_1.ttl"))
        whenever(turtleService.getPublicService(SERVICE_ID_2, false))
            .thenReturn(responseReader.readFile("no_meta_service_2.ttl"))

        val report = harvester.harvestServices(TEST_HARVEST_SOURCE, NEW_TEST_HARVEST_DATE)

        argumentCaptor<Model, String>().apply {
            verify(turtleService, times(1)).saveAsHarvestSource(first.capture(), second.capture())
            assertTrue(first.firstValue.isIsomorphicWith(responseReader.parseFile("harvest_response_0.ttl", "TURTLE")))
            Assertions.assertEquals(TEST_HARVEST_SOURCE.url, second.firstValue)
        }

        argumentCaptor<Model, String, Boolean>().apply {
            verify(turtleService, times(2)).saveAsPublicService(first.capture(), second.capture(), third.capture())
            assertTrue(checkIfIsomorphicAndPrintDiff(first.allValues[0], responseReader.parseFile("no_meta_service_0.ttl", "TURTLE"), "onlyRelevantUpdatedWhenHarvestedFromDB-norecords0"))
            assertTrue(checkIfIsomorphicAndPrintDiff(first.allValues[1], responseReader.parseFile("service_0_modified.ttl", "TURTLE"), "onlyRelevantUpdatedWhenHarvestedFromDB-0"))
            assertEquals(listOf(SERVICE_ID_0, SERVICE_ID_0), second.allValues)
            Assertions.assertEquals(listOf(false, true), third.allValues)
        }

        argumentCaptor<PublicServiceMeta>().apply {
            verify(metaRepository, times(1)).save(capture())
            assertEquals(SERVICE_META_0.copy(modified = NEW_TEST_HARVEST_DATE.timeInMillis), firstValue)
        }

        val expectedReport = HarvestReport(
            id="test-source",
            url="http://localhost:5000/fdk-public-service-publisher.ttl",
            dataType="publicService",
            harvestError=false,
            startTime = "2020-10-15 13:52:16 +0200",
            endTime = report!!.endTime,
            changedResources = listOf(FdkIdAndUri(fdkId="d5d0c07c-c14f-3741-9aa3-126960958cf0", uri="http://public-service-publisher.fellesdatakatalog.digdir.no/services/1"))
        )

        assertEquals(expectedReport, report)
    }

    @Test
    fun harvestWithErrorsIsNotPersisted() {
        whenever(adapter.fetchServices(TEST_HARVEST_SOURCE))
            .thenReturn(responseReader.readFile("harvest_error_response.ttl"))
        whenever(valuesMock.publicServiceHarvesterUri)
            .thenReturn("http://localhost:5000/public-services")

        val report = harvester.harvestServices(TEST_HARVEST_SOURCE, TEST_HARVEST_DATE)

        argumentCaptor<Model, String>().apply {
            verify(turtleService, times(0)).saveAsHarvestSource(first.capture(), second.capture())
        }

        argumentCaptor<Model, String, Boolean>().apply {
            verify(turtleService, times(0)).saveAsPublicService(first.capture(), second.capture(), third.capture())
        }

        argumentCaptor<PublicServiceMeta>().apply {
            verify(metaRepository, times(0)).save(capture())
        }

        val expectedReport = HarvestReport(
            id="test-source",
            url="http://localhost:5000/fdk-public-service-publisher.ttl",
            dataType="publicService",
            harvestError=true,
            errorMessage = "[line: 4, col: 3 ] Undefined prefix: dct",
            startTime = "2020-10-05 15:15:39 +0200",
            endTime = report!!.endTime
        )

        assertEquals(expectedReport, report)
    }

}