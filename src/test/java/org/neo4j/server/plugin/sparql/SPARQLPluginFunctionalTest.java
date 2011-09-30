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

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response.Status;

import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.server.rest.AbstractRestFunctionalTestBase;
import org.neo4j.test.GraphDescription.Graph;

public class SPARQLPluginFunctionalTest extends AbstractRestFunctionalTestBase
{
    private static final String ENDPOINT = "http://localhost:7474/db/data/ext/GremlinPlugin/graphdb/execute_script";

 
    
    private String formatGroovy( String script )
    {
        script = script.replace( ";", "\n" );
        if( !script.endsWith( "\n" ) )
        {
            script += "\n";
        }
        return "_Raw script source_\n\n" +
 "[source, groovy]\n" +
        		"----\n" +
        		script +
        		"----\n";
    }

    @Test
    @Ignore
    @Graph(value = {"A FOLLOW B", "B FOLLOW A", "B FOLLOW C"}, autoIndexNodes = true)
    public void followers() throws UnsupportedEncodingException
    {
        String script = "i = g.v("+data.get().get( "B" ).getId() +").out('FOLLOW')";
        gen()
        .expectedStatus( Status.OK.getStatusCode() )
        .description( formatGroovy( script ) );
        String response = gen().payload( "script=" + URLEncoder.encode( script, "UTF-8") )
        .payloadType( MediaType.APPLICATION_FORM_URLENCODED_TYPE )
        .post( ENDPOINT )
        .entity();
        assertTrue(response.contains( "you" ));
    }
}
