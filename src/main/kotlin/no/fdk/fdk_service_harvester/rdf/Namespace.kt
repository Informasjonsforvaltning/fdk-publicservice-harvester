package no.fdk.fdk_service_harvester.rdf

import org.apache.jena.rdf.model.ModelFactory
import org.apache.jena.rdf.model.Property

class CPSV {

    companion object {
        private val m = ModelFactory.createDefaultModel()
        val uri = "http://purl.org/vocab/cpsv#"
        val PublicService: Property = m.createProperty( "${uri}PublicService")
    }

}
