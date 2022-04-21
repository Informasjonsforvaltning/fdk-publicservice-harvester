package no.fdk.fdk_public_service_harvester.harvester

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import no.fdk.fdk_public_service_harvester.adapter.HarvestAdminAdapter
import no.fdk.fdk_public_service_harvester.model.HarvestAdminParameters
import no.fdk.fdk_public_service_harvester.rabbit.RabbitMQPublisher
import no.fdk.fdk_public_service_harvester.service.UpdateService
import org.slf4j.LoggerFactory
import org.springframework.boot.context.event.ApplicationReadyEvent
import org.springframework.context.event.EventListener
import org.springframework.stereotype.Service
import java.util.*

private val LOGGER = LoggerFactory.getLogger(HarvesterActivity::class.java)

@Service
class HarvesterActivity(
    private val harvestAdminAdapter: HarvestAdminAdapter,
    private val harvester: PublicServicesHarvester,
    private val publisher: RabbitMQPublisher,
    private val updateService: UpdateService
): CoroutineScope by CoroutineScope(Dispatchers.Default) {

    private val activitySemaphore = Semaphore(1)

    @EventListener
    fun fullHarvestOnStartup(event: ApplicationReadyEvent) = initiateHarvest(null)

    fun initiateHarvest(params: HarvestAdminParameters?) {
        if (params == null) LOGGER.debug("starting harvest of all services")
        else LOGGER.debug("starting harvest with parameters $params")

        launch {
            activitySemaphore.withPermit {
                harvestAdminAdapter.getDataSources(params ?: HarvestAdminParameters())
                    .filter { it.dataType == "publicService" }
                    .filter { it.url != null }
                    .map { async { harvester.harvestServices(it, Calendar.getInstance()) } }
                    .awaitAll()
                    .filterNotNull()
                    .also { updateService.updateUnionModel() }
                    .also {
                        if (params != null) LOGGER.debug("completed harvest with parameters $params")
                        else LOGGER.debug("completed harvest of all catalogs") }
                    .run { publisher.send(this) }
            }
        }
    }
}
