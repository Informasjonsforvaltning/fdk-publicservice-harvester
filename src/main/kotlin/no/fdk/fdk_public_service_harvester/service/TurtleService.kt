package no.fdk.fdk_public_service_harvester.service

import no.fdk.fdk_public_service_harvester.model.CatalogTurtle
import no.fdk.fdk_public_service_harvester.model.FDKCatalogTurtle
import no.fdk.fdk_public_service_harvester.model.FDKPublicServiceTurtle
import no.fdk.fdk_public_service_harvester.model.HarvestSourceTurtle
import no.fdk.fdk_public_service_harvester.model.PublicServiceTurtle
import no.fdk.fdk_public_service_harvester.rdf.createRDFResponse
import no.fdk.fdk_public_service_harvester.repository.CatalogTurtleRepository
import no.fdk.fdk_public_service_harvester.repository.FDKCatalogTurtleRepository
import no.fdk.fdk_public_service_harvester.repository.FDKPublicServiceTurtleRepository
import no.fdk.fdk_public_service_harvester.repository.HarvestSourceTurtleRepository
import no.fdk.fdk_public_service_harvester.repository.PublicServiceTurtleRepository
import org.apache.jena.rdf.model.Model
import org.apache.jena.riot.Lang
import org.springframework.data.repository.findByIdOrNull
import org.springframework.stereotype.Service
import java.io.ByteArrayOutputStream
import java.util.*
import java.util.zip.GZIPInputStream
import java.util.zip.GZIPOutputStream
import kotlin.text.Charsets.UTF_8

const val UNION_ID = "union-graph"

@Service
class TurtleService(
    private val harvestSourceRepository: HarvestSourceTurtleRepository,
    private val serviceRepository: PublicServiceTurtleRepository,
    private val fdkServiceRepository: FDKPublicServiceTurtleRepository,
    private val catalogRepository: CatalogTurtleRepository,
    private val fdkCatalogRepository: FDKCatalogTurtleRepository
) {

    fun saveAsCatalogUnion(model: Model, withRecords: Boolean) {
        if (withRecords) fdkCatalogRepository.save(model.createFDKCatalogTurtleDBO(UNION_ID))
        else catalogRepository.save(model.createCatalogTurtleDBO(UNION_ID))
    }

    fun getCatalogUnion(withRecords: Boolean): String? =
        if (withRecords) fdkCatalogRepository.findByIdOrNull(UNION_ID)
            ?.turtle
            ?.let { ungzip(it) }
        else catalogRepository.findByIdOrNull(UNION_ID)
            ?.turtle
            ?.let { ungzip(it) }

    fun saveAsServiceUnion(model: Model, withRecords: Boolean) {
        if (withRecords) fdkServiceRepository.save(model.createFDKPublicServiceTurtleDBO(UNION_ID))
        else serviceRepository.save(model.createPublicServiceTurtleDBO(UNION_ID))
    }

    fun getServiceUnion(withRecords: Boolean): String? =
        if (withRecords) fdkServiceRepository.findByIdOrNull(UNION_ID)
            ?.turtle
            ?.let { ungzip(it) }
        else serviceRepository.findByIdOrNull(UNION_ID)
            ?.turtle
            ?.let { ungzip(it) }

    fun saveAsCatalog(model: Model, fdkId: String, withRecords: Boolean) {
        if (withRecords) {
            fdkCatalogRepository.save(model.createFDKCatalogTurtleDBO(fdkId))
        }
        else {
            catalogRepository.save(model.createCatalogTurtleDBO(fdkId))
        }
    }

    fun getCatalog(fdkId: String, withRecords: Boolean): String? =
        if (withRecords) fdkCatalogRepository.findByIdOrNull(fdkId)
            ?.turtle
            ?.let { ungzip(it) }
        else catalogRepository.findByIdOrNull(fdkId)
            ?.turtle
            ?.let { ungzip(it) }

    fun saveAsPublicService(model: Model, fdkId: String, withRecords: Boolean) {
        if (withRecords) fdkServiceRepository.save(model.createFDKPublicServiceTurtleDBO(fdkId))
        else serviceRepository.save(model.createPublicServiceTurtleDBO(fdkId))
    }

    fun getPublicService(fdkId: String, withRecords: Boolean): String? =
        if (withRecords) fdkServiceRepository.findByIdOrNull(fdkId)
            ?.turtle
            ?.let { ungzip(it) }
        else serviceRepository.findByIdOrNull(fdkId)
            ?.turtle
            ?.let { ungzip(it) }

    fun saveAsHarvestSource(model: Model, uri: String) {
        harvestSourceRepository.save(model.createHarvestSourceTurtleDBO(uri))
    }

    fun getHarvestSource(uri: String): String? =
        harvestSourceRepository.findByIdOrNull(uri)
            ?.turtle
            ?.let { ungzip(it) }

}

private fun Model.createCatalogTurtleDBO(id: String): CatalogTurtle =
    CatalogTurtle(
        id = id,
        turtle = gzip(createRDFResponse(Lang.TURTLE))
    )

private fun Model.createFDKCatalogTurtleDBO(id: String): FDKCatalogTurtle =
    FDKCatalogTurtle(
        id = id,
        turtle = gzip(createRDFResponse(Lang.TURTLE))
    )

private fun Model.createPublicServiceTurtleDBO(id: String): PublicServiceTurtle =
    PublicServiceTurtle(
        id = id,
        turtle = gzip(createRDFResponse(Lang.TURTLE))
    )

private fun Model.createFDKPublicServiceTurtleDBO(id: String): FDKPublicServiceTurtle =
    FDKPublicServiceTurtle(
        id = id,
        turtle = gzip(createRDFResponse(Lang.TURTLE))
    )

private fun Model.createHarvestSourceTurtleDBO(uri: String): HarvestSourceTurtle =
    HarvestSourceTurtle(
        id = uri,
        turtle = gzip(createRDFResponse(Lang.TURTLE))
    )

fun gzip(content: String): String {
    val bos = ByteArrayOutputStream()
    GZIPOutputStream(bos).bufferedWriter(UTF_8).use { it.write(content) }
    return Base64.getEncoder().encodeToString(bos.toByteArray())
}

fun ungzip(base64Content: String): String {
    val content = Base64.getDecoder().decode(base64Content)
    return GZIPInputStream(content.inputStream())
        .bufferedReader(UTF_8)
        .use { it.readText() }
}
