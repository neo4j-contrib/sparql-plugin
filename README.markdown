This is a [Neo4j Server](http://neo4j.com/download/) plugin, providing [Sparql](http://en.wikipedia.org/wiki/SPARQL)
 [Neo4j Server](http://neo4j.com/). This effectively turns the Neo4j Server into a Triple Store.
 
For usage via the REST API, see the [current documentation](http://neo4j-contrib.github.io/sparql-plugin/).

Building from source and deploying into Neo4j Server
-----------------------------------------------------

    mvn clean package
    unzip target/neo4j-sparql-plugin-0.2-SNAPSHOT-server-plugin.zip -d $NEO4J_HOME/plugins/sparql-plugin
    cd $NEO4J_HOME
    bin/neo4j restart
