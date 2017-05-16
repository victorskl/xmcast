## Reliable Multicast over IP Multicast
 
 * Experimental implementation of "Reliable Multicast over IP Multicast" Section 15.4.2, pg.665 as described in book _Distributed Systems - Concepts and Design_ 5th ed [Coulouris et al.]
 * This is also Reliable FIFO Ordered Multicast discussed in pg.669
 * Better to observe with IntelliJ or Eclipse IDE but it a maven project, like so:
 
    ```
    mvn clean package
    java -jar target\xmcast-1.0-SNAPSHOT-jar-with-dependencies.jar
    ```