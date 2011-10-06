/**
 * Copyright (c) 2002-2011 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.server.plugin.sparql;

import info.aduna.iteration.CloseableIteration;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import org.junit.Ignore;
import org.junit.Test;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.impl.EmptyBindingSet;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.QueryParser;
import org.openrdf.query.parser.sparql.SPARQLParserFactory;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.repository.sail.SailRepositoryConnection;
import org.openrdf.rio.RDFFormat;
import org.openrdf.sail.Sail;
import org.openrdf.sail.SailConnection;

import com.tinkerpop.blueprints.pgm.impls.neo4j.Neo4jGraph;
import com.tinkerpop.blueprints.pgm.oupls.sail.GraphSail;

public class BerlinDatasetTest
{

    private static String dB_DIR = "target/berlindb";

    /**
     * the queries are coming from
     * http://www4.wiwiss.fu-berlin.de/bizer/BerlinSPARQLBenchmark
     * /spec/ExploreUseCase/index.html
     */
    @Test
    public void berlinQuery() throws Exception
    {
        loadTriples();
        Sail sail = new GraphSail( new Neo4jGraph( dB_DIR ) );
        sail.initialize();
        Map<String, String> queries = new HashMap<String, String>();
        queries.put(
                "q1",
                "PREFIX bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/> "
                        + "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> "
                        + "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> "
                        + "SELECT DISTINCT ?product ?label "
                        + "WHERE { "
                        + "?product rdfs:label ?label . "
                        + "?product rdf:type <http://www4.wiwiss.F-berlin.de/bizer/bsbm/v01/instances/ProductType2> . "
                        + "?product bsbm:productFeature <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductFeature12>. "
                        + "?product bsbm:productFeature <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductFeature20> . "
                        + "?product bsbm:productPropertyNumeric1 ?value1 . "
                        + "FILTER (?value1 > 348)} " + "ORDER BY ?label "
                        + "LIMIT 10 " );

        queries.put(
                "q2",
                "PREFIX bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/> "
                        + "PREFIX inst: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer1/> "
                        + "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> "
                        + "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> "
                        + "PREFIX dc: <http://purl.org/dc/elements/1.1/> "
                        + "SELECT ?label ?comment ?producer ?productFeature ?propertyTextual1 "
                        + "?propertyTextual2 ?propertyTextual3 ?propertyNumeric1 "
                        + "?propertyNumeric2 ?propertyTextual4 ?propertyTextual5 "
                        + "?propertyNumeric4 "
                        + "WHERE { "
                        + "inst:Product5  rdfs:label ?label . "
                        + "inst:Product5  rdfs:comment ?comment . "
                        + "inst:Product5  bsbm:producer ?p . "
                        + "?p rdfs:label ?producer . "
                        + "inst:Product5  dc:publisher ?p . "
                        + "inst:Product5  bsbm:productFeature ?f . "
                        + "?f rdfs:label ?productFeature . "
                        + "inst:Product5  bsbm:productPropertyTextual1 ?propertyTextual1 . "
                        + "inst:Product5  bsbm:productPropertyTextual2 ?propertyTextual2 . "
                        + "inst:Product5  bsbm:productPropertyTextual3 ?propertyTextual3 . "
                        + "inst:Product5  bsbm:productPropertyNumeric1 ?propertyNumeric1 . "
                        + "inst:Product5  bsbm:productPropertyNumeric2 ?propertyNumeric2 . "
                        + "OPTIONAL {inst:Product5  bsbm:productPropertyTextual4 ?propertyTextual4 } "
                        + "OPTIONAL {inst:Product5  bsbm:productPropertyTextual5 ?propertyTextual5 } "
                        + "OPTIONAL {inst:Product5  bsbm:productPropertyNumeric4 ?propertyNumeric4 }} " );

        queries.put(
                "q3",
                "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> "
                        + "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> "
                        + "PREFIX bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/> "
                        + "SELECT ?product ?label "
                        + "WHERE { "
                        + "?product rdfs:label ?label . "
                        + "?product rdf:type <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductType2>   . "
                        + "?product bsbm:productFeature <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductFeature12> . "
                        + "?product bsbm:productPropertyNumeric1 ?p1 . "
                        + "FILTER ( ?p1 > 214 ) "
                        + "?product bsbm:productPropertyNumeric3 ?p3 . "
                        + "FILTER (?p3 < 698 )"
                        + "OPTIONAL { "
                        + "?product bsbm:productFeature <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductFeature20> . "
                        + "?product rdfs:label ?testVar } "
                        + "FILTER (!bound(?testVar)) } " + "ORDER BY ?label "
                        + "LIMIT 10 " );

        queries.put(
                "q4",
                "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> "
                        + "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> "
                        + "PREFIX bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/> "
                        + "SELECT ?product ?label "
                        + "WHERE { "
                        + "{ ?product rdfs:label ?label . "
                        + "?product rdf:type <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductType2> . "
                        + "?product bsbm:productFeature <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductFeature12>. "
                        + "?product bsbm:productFeature <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductFeature20> . "
                        + "?product bsbm:productPropertyNumeric1 ?value1 . "
                        + "FILTER (?value1 > 348)} "
                        + "UNION "
                        + "{ "
                        + "?product rdfs:label ?label . "
                        + "?product rdf:type  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductType1>. "
                        + "?product bsbm:productFeature  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductFeature12> . "
                        + "?product bsbm:productFeature  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductFeature8> . "
                        + "?product bsbm:productPropertyNumeric2 ?p2 . "
                        + "FILTER ( ?p2>759 ) " + "} " + "} "
                        // + "ORDER BY ?label LIMIT 10" );
                        + "ORDER BY ?label " + "LIMIT 10  " + "OFFSET 10 " );

        queries.put(
                "q5",
                "PREFIX bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/> "
                        + "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> "
                        + "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> "
                        + "PREFIX inst: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer1/> "
                        + "SELECT DISTINCT ?product ?productLabel "
                        + "WHERE { "
                        + "?product rdfs:label ?productLabel . "
                        + "FILTER (inst:Product6  != ?product) "
                        + "inst:Product6  bsbm:productFeature ?prodFeature . "
                        + "?product bsbm:productFeature ?prodFeature . "
                        + "inst:Product6  bsbm:productPropertyNumeric1 ?origProperty1 . "
                        + "?product bsbm:productPropertyNumeric1 ?simProperty1 . "
                        + "FILTER (?simProperty1 < (?origProperty1 + 120) && ?simProperty1 > "
                        + "(?origProperty1 - 120)) "
                        + "inst:Product6  bsbm:productPropertyNumeric2 ?origProperty2 . "
                        + "?product bsbm:productPropertyNumeric2 ?simProperty2 . "
                        + "FILTER (?simProperty2 < (?origProperty2 + 170) && ?simProperty2 > "
                        + "(?origProperty2 - 170)) } "
                        + "ORDER BY ?productLabel " + "LIMIT 5 " );

        queries.put(
                "q6",
                ""
                        + "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> "
                        + "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> "
                        + "PREFIX bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/> "
                        + "SELECT ?product ?label " + "WHERE {"
                        + "    ?product rdfs:label ?label ."
                        + "    ?product rdf:type bsbm:Product ."
                        + "    FILTER regex(?label, \"r\")}" );

        queries.put(
                "q7",
                "PREFIX bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/> "
                        + "PREFIX foaf: <http://xmlns.com/foaf/0.1/> "
                        + "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> "
                        + "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> "
                        + "PREFIX dc: <http://purl.org/dc/elements/1.1/> "
                        + "PREFIX rev: <http://purl.org/stuff/rev#> "
                        + "SELECT ?productLabel ?offer ?price ?vendor ?vendorTitle ?review "
                        + "?revTitle ?reviewer ?revName ?rating1 ?rating2 "
                        + "WHERE { "
                        + "<http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer1/Product5>  rdfs:label ?productLabel . "
                        + "OPTIONAL { "
                        + "?offer bsbm:product <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer1/Product5>  . "
                        + "?offer bsbm:price ?price . "
                        + "?offer bsbm:vendor ?vendor . "
                        + "?vendor rdfs:label ?vendorTitle . "
                        + "?vendor bsbm:country <http://downlode.org/rdf/iso-3166/countries#DE>. "
                        + "?offer dc:publisher ?vendor . "
                        + "?offer bsbm:validTo ?date . "
                        + "FILTER (?date > 2001-09-16 ) } "
                        + "OPTIONAL { "
                        + "?review bsbm:reviewFor <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer1/Product5>  . "
                        + "?review rev:reviewer ?reviewer . "
                        + "?reviewer foaf:name ?revName . "
                        + "?review dc:title ?revTitle . "
                        + "OPTIONAL { ?review bsbm:rating1 ?rating1 . } "
                        + "OPTIONAL { ?review bsbm:rating2 ?rating2 . } } } " );

        queries.put(
                "q8",
                "PREFIX bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/> "
                        + "PREFIX inst:<http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer1/> "
                        + "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> "
                        + "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> "
                        + "PREFIX dc: <http://purl.org/dc/elements/1.1/> "
                        + "PREFIX rev: <http://purl.org/stuff/rev#> "
                        + "PREFIX foaf: <http://xmlns.com/foaf/0.1/> "
                        + "SELECT ?title ?text ?reviewDate ?reviewer ?reviewerName ?rating1 "
                        + "?rating2 ?rating3 ?rating4 " + "WHERE { "
                        + "?review bsbm:reviewFor inst:Product6  . "
                        + "?review dc:title ?title . "
                        + "?review rev:text ?text . "
                        + "FILTER langMatches( lang(?text), 'EN' ) "
                        + "?review bsbm:reviewDate ?reviewDate . "
                        + "?review rev:reviewer ?reviewer . "
                        + "?reviewer foaf:name ?reviewerName . "
                        + "OPTIONAL { ?review bsbm:rating1 ?rating1 . } "
                        + "OPTIONAL { ?review bsbm:rating2 ?rating2 . } "
                        + "OPTIONAL { ?review bsbm:rating3 ?rating3 . } "
                        + "OPTIONAL { ?review bsbm:rating4 ?rating4 . } } "
                        + "ORDER BY DESC(?reviewDate) " + "LIMIT 20 " );

        queries.put(
                "q9",
                "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> "
                        + "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> "
                        + "PREFIX rev: <http://purl.org/stuff/rev#> "
                        + "DESCRIBE ?x "
                        + "WHERE { "
                        + "<http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromRatingSite1/Review4>  rev:reviewer ?x } " );

        queries.put(
                "q10",
                "PREFIX dc: <http://purl.org/dc/elements/1.1/> "
                        + "PREFIX bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/> "
                        + "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#> "
                        + "SELECT DISTINCT ?offer ?price "
                        + "WHERE { "
                        + "?offer bsbm:product <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer1/Product1> . "
                        + "?offer bsbm:vendor ?vendor . "
                        + "?offer dc:publisher ?vendor . "
                        + "?vendor bsbm:country  <http://downlode.org/rdf/iso-3166/countries#GB>  . "
                        + "?offer bsbm:deliveryDays ?deliveryDays . "
                        + "FILTER (?deliveryDays <= 3) "
                        + "?offer bsbm:price ?price . "
                        + "?offer bsbm:validTo ?date . "
                        + "FILTER (?date > '2008-04-19T00:00:00'^^xsd:dateTime) "
                        + "} " + "ORDER BY xsd:double(str(?price)) "
                        + "LIMIT 10 " );

        queries.put(
                "q11",
                " SELECT ?property ?hasValue ?isValueOf "
                        + "WHERE { "
                        + "{ <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor1/Offer1>  ?property ?hasValue } "
                        + "UNION "
                        + "{ ?isValueOf ?property <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor1/Offer1>  } } " );

        queries.put(
                "q12",
                " PREFIX bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/> "
                        + "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> "
                        + "PREFIX foaf: <http://xmlns.com/foaf/0.1/> "
                        + "PREFIX bsbm-export: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/export/> "
                        + "CONSTRUCT { "
                        + "<http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor1/Offer1> bsbm-export:product ?productURI . "
                        + "<http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor1/Offer1> bsbm-export:productlabel ?productlabel . "
                        + "<http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor1/Offer1> bsbm-export:vendor ?vendorname . "
                        + "<http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor1/Offer1> bsbm-export:vendorhomepage ?vendorhomepage . "
                        + "<http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor1/Offer1> bsbm-export:offerURL ?offerURL . "
                        + "<http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor1/Offer1> bsbm-export:price ?price . "
                        + "<http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor1/Offer1> bsbm-export:deliveryDays ?deliveryDays . "
                        + "<http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor1/Offer1> bsbm-export:validuntil ?validTo } "
                        + "WHERE { "
                        + "<http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor1/Offer1>bsbm:product ?productURI . "
                        + "?productURI rdfs:label ?productlabel . "
                        + "<http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor1/Offer1> bsbm:vendor ?vendorURI . "
                        + "?vendorURI rdfs:label ?vendorname . "
                        + "?vendorURI foaf:homepage ?vendorhomepage . "
                        + "<http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor1/Offer1> bsbm:offerWebpage ?offerURL . "
                        + "<http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor1/Offer1> bsbm:price ?price . "
                        + "<http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor1/Offer1> bsbm:deliveryDays ?deliveryDays . "
                        + "<http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor1/Offer1> bsbm:validTo ?validTo } " );
        QueryParser parser = new SPARQLParserFactory().getParser();
        ParsedQuery query = null;
        CloseableIteration<? extends BindingSet, QueryEvaluationException> sparqlResults;
        SailConnection conn = sail.getConnection();
        for ( String key : queries.keySet() )
            try
            {
                query = parser.parseQuery( queries.get( key ),
                        "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/" );
                sparqlResults = conn.evaluate( query.getTupleExpr(),
                        query.getDataset(), new EmptyBindingSet(), false );
                System.out.println( "Results --- " + key );
                while ( sparqlResults.hasNext() )
                {
                    System.out.println( "Result: " + sparqlResults.next() );
                }
            }
            catch ( Throwable e )
            {
                System.out.println( "SailException " + key );
                e.printStackTrace();
            }
        conn.close();
        sail.shutDown();
    }

    @Test
    @Ignore
    public void loadTriples() throws Exception
    {
//        Neo4jBatchGraph neo = new Neo4jBatchGraph( dB_DIR+"_batch" );
        Neo4jGraph neo = new Neo4jGraph( dB_DIR );
        neo.setMaxBufferSize( 20000 );
        Sail sail = new GraphSail( neo );
        sail.initialize();
        SailRepositoryConnection connection;
        try
        {
            connection = new SailRepository( sail ).getConnection();
            File file = new File( "berlin_nt_100.nt" );
            System.out.println( "Loading " + file + ": " );
            connection.add( file, null, RDFFormat.NTRIPLES );
            connection.close();
        }
        catch ( RepositoryException e1 )
        {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }
        System.out.print( "Done." );
        sail.shutDown();
        neo.shutdown();
    }
}
