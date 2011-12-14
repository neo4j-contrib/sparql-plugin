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

import static org.junit.Assert.*;

import java.io.UnsupportedEncodingException;

import org.junit.Before;
import org.junit.Test;
import org.neo4j.server.rest.AbstractRestFunctionalTestBase;
import org.openrdf.repository.RepositoryException;

import com.sun.jersey.api.client.ClientResponse.Status;
import com.tinkerpop.blueprints.pgm.impls.neo4j.Neo4jGraph;

public class SPARQLPluginFunctionalTest extends AbstractRestFunctionalTestBase
{
    private static final String ENDPOINT = "http://localhost:7474/db/data/ext/SPARQLPlugin/graphdb/execute_sparql";

 
    @Before
    public void doData() throws Exception, RepositoryException {
        SPARQLPluginTest.insertData( new Neo4jGraph( graphdb(), true ) );
    }
    @Test
    public void followers() throws UnsupportedEncodingException
    {
        String payload = "{\"query\":\"SELECT ?x ?y WHERE { ?x <http://neo4j.org#knows> ?y .}\"}";
        
        String entity = gen.get().payload( payload ).expectedStatus( Status.OK ).post( ENDPOINT ).entity();
        assertTrue(entity.contains( "http://neo4j.org#sara" ));
        System.out.println(entity);
    }
}
