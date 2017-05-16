package com.sankholin.xmcast;

import java.net.InetAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ProcessTest {

    public static void main(String[] args) {

        try {
            // Simulate N concurrent Processes with the multicast group "224.2.2.5:8888"
            int N = 3;
            Process[] processes = new Process[N];
            ExecutorService pool = Executors.newFixedThreadPool(N);
            for (int i = 0; i < N; i++) {
                Process p = new Process(InetAddress.getByName("224.2.2.5"), 8888);
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

            // Sequential sends without delay
            processes[0].send(String.format("Meerkat @ [%s]", Process.ts()));
            processes[0].send(String.format("Rabbit @ [%s]", Process.ts()));
            processes[0].send(String.format("Koala @ [%s]", Process.ts()));
            processes[0].send(String.format("Chicken @ [%s]", Process.ts()));
            processes[0].send(String.format("Panda @ [%s]", Process.ts()));
            processes[0].send(String.format("Tiger @ [%s]", Process.ts()));
            processes[0].send(String.format("Elephant @ [%s]", Process.ts()));

            // The goal here is to test Reliable Multicast,
            // and optionally observe FIFO Ordered on sequence number i.e. SGP

        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
