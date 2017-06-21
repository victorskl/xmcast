package com.sankholin.xmcast;

import java.net.InetAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.sankholin.xmcast.Util.ts;

public class ProcessTest {

    public static void main(String[] args) {

        try {
            // Simulate N concurrent Processes with the multicast group "224.2.2.5:8888"
            int N = 3;
            Process[] processes = new Process[N];
            ExecutorService pool = Executors.newFixedThreadPool(N);
            for (int i = 0; i < N; i++) {
                Process p = new Process(String.valueOf(i), InetAddress.getByName("224.2.2.5"), 8888);
                pool.execute(p);
                processes[i] = p;
            }

            int W = 3;
            Thread.sleep(1000 * W); // let wait server processes up

            // Now simulate processes start sending messages each other

            processes[0].send("First message");
            Thread.sleep(1000);
            processes[0].send("Second message");
            Thread.sleep(3000);
            processes[0].send("Third message");
            Thread.sleep(2000);
            processes[2].send("Foo Bar");
            Thread.sleep(2000);
            processes[2].send("@Alan is hungry!!");
            Thread.sleep(1000);

            // Sequential sends without delay - though it execute sequential but it mimic concurrent
            // bec CPU is way faster the human eye observability in real time!
            // However, comment these sends if you feel confuse and observe above with time delay first
            processes[0].send(String.format("Meerkat @ [%s]", ts()));
            processes[0].send(String.format("Rabbit @ [%s]", ts()));
            processes[0].send(String.format("Koala @ [%s]", ts()));
            processes[0].send(String.format("Chicken @ [%s]", ts()));
            processes[0].send(String.format("Panda @ [%s]", ts()));
            processes[0].send(String.format("Tiger @ [%s]", ts()));
            processes[0].send(String.format("Elephant @ [%s]", ts()));

            // The goal here is to test Reliable Multicast,
            // and optionally observe FIFO Ordered on sequence number i.e. SGP

        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
