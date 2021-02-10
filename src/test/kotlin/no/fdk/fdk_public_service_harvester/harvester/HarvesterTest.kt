package no.fdk.fdk_public_service_harvester.harvester

import com.nhaarman.mockitokotlin2.*
import no.fdk.fdk_public_service_harvester.adapter.FusekiAdapter
import no.fdk.fdk_public_service_harvester.adapter.ServicesAdapter
import no.fdk.fdk_public_service_harvester.configuration.ApplicationProperties
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

        harvester.harvestServices(TEST_HARVEST_SOURCE, TEST_HARVEST_DATE)

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

        harvester.harvestServices(TEST_HARVEST_SOURCE, TEST_HARVEST_DATE)

        argumentCaptor<Model, String>().apply {
            verify(turtleService, times(0)).saveAsHarvestSource(first.capture(), second.capture())
        }

        argumentCaptor<Model, String, Boolean>().apply {
            verify(turtleService, times(0)).saveAsPublicService(first.capture(), second.capture(), third.capture())
        }

        argumentCaptor<PublicServiceMeta>().apply {
            verify(metaRepository, times(0)).save(capture())
        }
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

        harvester.harvestServices(TEST_HARVEST_SOURCE, NEW_TEST_HARVEST_DATE)

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

    }

    @Test
    fun harvestWithErrorsIsNotPersisted() {
        whenever(adapter.fetchServices(TEST_HARVEST_SOURCE))
            .thenReturn(responseReader.readFile("harvest_error_response.ttl"))
        whenever(valuesMock.publicServiceHarvesterUri)
            .thenReturn("http://localhost:5000/public-services")

        harvester.harvestServices(TEST_HARVEST_SOURCE, TEST_HARVEST_DATE)

        argumentCaptor<Model, String>().apply {
            verify(turtleService, times(0)).saveAsHarvestSource(first.capture(), second.capture())
        }

        argumentCaptor<Model, String, Boolean>().apply {
            verify(turtleService, times(0)).saveAsPublicService(first.capture(), second.capture(), third.capture())
        }

        argumentCaptor<PublicServiceMeta>().apply {
            verify(metaRepository, times(0)).save(capture())
        }
    }

}