/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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

import com.tinkerpop.blueprints.KeyIndexableGraph;
import com.tinkerpop.blueprints.impls.neo4j2.Neo4j2Graph;
import com.tinkerpop.blueprints.oupls.sail.GraphSail;
import info.aduna.iteration.CloseableIteration;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.server.plugins.*;
import org.neo4j.server.rest.repr.*;
import org.openrdf.model.ValueFactory;
import org.openrdf.query.Binding;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.impl.EmptyBindingSet;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.sparql.SPARQLParser;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.repository.sail.SailRepositoryConnection;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;


@Description("A server side SPARQL plugin for the Neo4j REST server")
public class SPARQLPlugin extends ServerPlugin {

    private GraphSail sail;
    private SPARQLParser parser;
    private SailRepositoryConnection sc;
    private Neo4j2Graph neo4jGraph;

    @Name("execute_sparql")
    @Description("execute a SPARQL query.")
    @PluginTarget(GraphDatabaseService.class)
    public Representation executeSPARQL(
            @Source final GraphDatabaseService neo4j,
            @Description("The SPARQL query") @Parameter(name = "query", optional = false) final String queryString,
            @Description("JSON Map of additional parameters for the query") @Parameter(name = "params", optional = true) final Map params) {

        initSail(neo4j);
        try (Transaction tx = neo4j.beginTx()) {

            ParsedQuery query = null;
            CloseableIteration<? extends BindingSet, QueryEvaluationException> sparqlResults;

            query = parser.parseQuery(queryString, "http://neo4j.org");
            sparqlResults = sail.getConnection().evaluate(
                    query.getTupleExpr(), query.getDataset(),
                    new EmptyBindingSet(), false);
            ArrayList results = new ArrayList();
            while (sparqlResults.hasNext()) {
                BindingSet next = sparqlResults.next();
                Map mapResults = new HashMap();
                for (Binding binding : next) {
                    mapResults.put(binding.getName(), binding.getValue());
                }
                results.add(mapResults);
            }
            Representation representation = SparqlObjectToRepresentationConverter.convert(results);
            tx.success();
            return representation;
        } catch (final Exception e) {
            e.printStackTrace();
            return ValueRepresentation.string(e.getMessage());
            // return new ExceptionRepresentation( e ) );
        }
    }

    private void initSail(GraphDatabaseService neo4j) {
        if (sail == null) {
            neo4jGraph = new Neo4j2Graph(neo4j);
            sail = new GraphSail<KeyIndexableGraph>(neo4jGraph);
            try {
                sail.initialize();
                sc = new SailRepository(sail).getConnection();
                parser = new SPARQLParser();
            } catch (Exception e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }

    }

    @Name("insert_quad")
    @Description("execute a SPARQL query.")
    @PluginTarget(GraphDatabaseService.class)
    public Representation executeInsert(
            @Source final GraphDatabaseService neo4j,
            @Description("Subject") @Parameter(name = "s", optional = false) final String s,
            @Description("Predicate") @Parameter(name = "p", optional = false) final String p,
            @Description("Object") @Parameter(name = "o", optional = false) final String o,
            @Description("Context") @Parameter(name = "c", optional = false) final String c) {
        initSail(neo4j);
        ValueFactory vf = sail.getValueFactory();
        try {
            try {
                sc.add(vf.createURI(s), vf.createURI(p), vf.createURI(o),
                        vf.createURI(c));
            } catch (IllegalArgumentException ia) {
                sc.add(vf.createURI(s), vf.createURI(p), vf.createLiteral(o),
                        vf.createURI(c));
            }
            sc.commit();

        } catch (RepositoryException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return ValueRepresentation.emptyRepresentation();
    }

}
