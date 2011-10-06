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

import java.util.ArrayList;
import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.server.plugins.Description;
import org.neo4j.server.plugins.Name;
import org.neo4j.server.plugins.Parameter;
import org.neo4j.server.plugins.PluginTarget;
import org.neo4j.server.plugins.ServerPlugin;
import org.neo4j.server.plugins.Source;
import org.neo4j.server.rest.repr.ListRepresentation;
import org.neo4j.server.rest.repr.Representation;
import org.neo4j.server.rest.repr.ValueRepresentation;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.impl.EmptyBindingSet;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.sparql.SPARQLParser;
import org.openrdf.sail.Sail;

import com.tinkerpop.blueprints.pgm.impls.neo4j.Neo4jGraph;
import com.tinkerpop.blueprints.pgm.oupls.sail.GraphSail;

@Description( "A server side SPARQL plugin for the Neo4j REST server" )
public class SPARQLPlugin extends ServerPlugin
{

    @Name( "execute_sparql" )
    @Description( "execute a SPARQL query." )
    @PluginTarget( GraphDatabaseService.class )
    public Representation executeSPARQL(
            @Source final GraphDatabaseService neo4j,
            @Description( "The SPARQL query" ) @Parameter( name = "query", optional = false ) final String queryString,
            @Description( "JSON Map of additional parameters for the query" ) @Parameter( name = "params", optional = true ) final Map params )
    {

        try
        {
            Sail sail = new GraphSail( new Neo4jGraph( neo4j, false ) );
            sail.initialize();
            SPARQLParser parser = new SPARQLParser();
            ParsedQuery query = null;
            CloseableIteration<? extends BindingSet, QueryEvaluationException> sparqlResults;

            query = parser.parseQuery( queryString, "http://neo4j.org" );
            sparqlResults = sail.getConnection().evaluate(
                    query.getTupleExpr(), query.getDataset(),
                    new EmptyBindingSet(), false );
            ArrayList<String> results = new ArrayList<String>();
            while ( sparqlResults.hasNext() )
            {
                results.add( sparqlResults.next().toString() );
            }
            return ListRepresentation.string( results );
        }
        catch ( final Exception e )
        {
            e.printStackTrace();
            return ValueRepresentation.string( e.getMessage() );
            // return new ExceptionRepresentation( e ) );
        }
    }

}
