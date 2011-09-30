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

import java.util.HashMap;
import java.util.Map;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.server.rest.repr.OutputFormat;
import org.neo4j.server.rest.repr.Representation;
import org.neo4j.test.ImpermanentGraphDatabase;
import org.openrdf.model.Statement;
import org.openrdf.model.ValueFactory;
import org.openrdf.sail.Sail;
import org.openrdf.sail.SailConnection;
import org.openrdf.sail.SailException;

import com.tinkerpop.blueprints.pgm.impls.neo4j.Neo4jGraph;
import com.tinkerpop.blueprints.pgm.oupls.sail.GraphSail;

public class SPARQLPluginTest
{

    private static ImpermanentGraphDatabase neo4j = null;
    private static SPARQLPlugin plugin = null;
    private static OutputFormat json = null;
    private static JSONParser parser = new JSONParser();

    @BeforeClass
    public static void setUpBeforeClass() throws Exception
    {
        neo4j = new ImpermanentGraphDatabase( "target/db" );
        System.setProperty( "org.openrdf.repository.debug", "true" );
        plugin = new SPARQLPlugin();
        Sail sail = new GraphSail(new Neo4jGraph( neo4j ));
        sail.initialize();
        SailConnection sc = sail.getConnection();
        ValueFactory vf = sail.getValueFactory();
        sc.addStatement(vf.createURI("http://neo4j.org#joe"), vf.createURI("http://neo4j.org#knows"), vf.createURI("http://neo4j.org#sara"), vf.createURI("http://neo4j.org"));
        sc.addStatement(vf.createURI("http://neo4j.org#joe"), vf.createURI("http://neo4j.org#name"), vf.createLiteral("joe"), vf.createURI("http://neo4j.org"));
        sc.commit();
        CloseableIteration<? extends Statement, SailException> results = sc.getStatements(vf.createURI("http://neo4j.org#joe"), null, null, false);
        while(results.hasNext()) {
            System.out.println(results.next());
        }
        sc.close();
//        sail.shutDown();

            }

    private static Representation executeTestScript(final String script, Map params)
    {
            return plugin.executeSPARQL( neo4j, script, params );
    }

    @Test
    public void executeSelect() throws Exception
    {
        JSONObject object = (JSONObject) parser.parse( json.format( SPARQLPluginTest.executeTestScript( "SELECT ?x ?y WHERE { ?x <http://neo4j.org#knows> ?y }", new HashMap()) ) );
        // Assert.assertEquals(
        //       ( (JSONObject) object.get( "data" ) ).get( "name" ), "sara" );
    }

}
