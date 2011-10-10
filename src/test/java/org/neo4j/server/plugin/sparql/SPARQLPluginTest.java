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

import static org.junit.Assert.assertTrue;
import info.aduna.iteration.CloseableIteration;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.server.rest.repr.OutputFormat;
import org.neo4j.server.rest.repr.Representation;
import org.neo4j.server.rest.repr.formats.JsonFormat;
import org.openrdf.model.Statement;
import org.openrdf.model.ValueFactory;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.repository.sail.SailRepositoryConnection;
import org.openrdf.sail.Sail;

import com.tinkerpop.blueprints.pgm.impls.neo4j.Neo4jGraph;
import com.tinkerpop.blueprints.pgm.oupls.sail.GraphSail;

public class SPARQLPluginTest
{

    private static SPARQLPlugin plugin = null;
    private static OutputFormat json = null;
    private static GraphDatabaseService neo4j;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception
    {
        json = new OutputFormat( new JsonFormat(), new URI( "http://localhost/" ), null );
        Neo4jGraph neo1 = new Neo4jGraph( new EmbeddedGraphDatabase( "target/db1" ), true );
        neo1.setMaxBufferSize( 20000 );
//        Neo4jBatchGraph neo1 = new Neo4jBatchGraph( "target/db1" );
        Sail sail = new GraphSail(neo1);
        sail.initialize();
        SailRepositoryConnection sc = new SailRepository( sail ).getConnection();
        ValueFactory vf = sail.getValueFactory();
        sc.add( vf.createURI( "http://neo4j.org#joe" ),
                vf.createURI( "http://neo4j.org#knows" ),
                vf.createURI( "http://neo4j.org#sara" ),
                vf.createURI( "http://neo4j.org" ) );
        sc.add( vf.createURI( "http://neo4j.org#joe" ),
                vf.createURI( "http://neo4j.org#name" ),
                vf.createLiteral( "joe" ), vf.createURI( "http://neo4j.org" ) );
        sc.commit();
        CloseableIteration<Statement, RepositoryException> results = sc.getStatements(
                vf.createURI( "http://neo4j.org#joe" ), null, null, false );
        while ( results.hasNext() )
        {
            System.out.println( results.next() );
        }
        sc.close();
        sail.shutDown();
        neo1.shutdown();
        plugin = new SPARQLPlugin();
        neo4j = new EmbeddedGraphDatabase( "target/db1" );

    }

    private static Representation executeTestScript( final String script,
            Map params )
    {
        return plugin.executeSPARQL( neo4j, script, params );
    }

    private static String queryString = ""
                                        + "SELECT ?x ?y " + "WHERE { "
                                        + "?x <http://neo4j.org#knows> ?y ."
                                        + "}";

    @Test
    public void executeSelect() throws Exception
    {
         Representation result = SPARQLPluginTest.executeTestScript(
         queryString, new HashMap() );
        String format = json.format(
         result );
         assertTrue(format.contains( "sara" ));
         assertTrue(format.contains( "joe" ));
    }

    @AfterClass
    public static void cleanUp()
    {
        neo4j.shutdown();
    }

}
