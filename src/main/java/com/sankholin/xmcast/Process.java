package com.sankholin.xmcast;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Experimental implementation of "Reliable Multicast over IP Multicast" Section 15.4.2, pg.665
 * as described in book Distributed Systems - Concepts and Design 5th ed [Coulouris et al.]
 *
 * This also is Reliable FIFO Ordered Multicast discussed in pg.669
 */
public class Process implements Runnable {

    private static int RCV_BUF = 2048;

    private String pid;
    private InetAddress group;
    private int port;
    private MulticastSocket socket;

    private Long sgp = 1L;
    private Map<String, Long> rgq;
    private Map<String, Long> rgp;

    private JSONParser parser = new JSONParser();

    private Queue<String> holdBackQueue = new LinkedList<>();

    public Process(InetAddress group, int port) throws IOException {
        this.pid = UUID.randomUUID().toString();
        this.group = group;
        this.port = port;
        this.socket = new MulticastSocket(this.port);
        this.socket.joinGroup(group);
        this.rgp = new ConcurrentHashMap<>();
        this.rgq = new ConcurrentHashMap<>();
    }

    public void send(String message) {

        /*
         Possible fix for error in book, see the awkward situation note below.
         We may want to increment sequence number first, if SGP initialize with zero.
         The book said:
          " The sequence number is initially zero." at pg.649
        */
        // sgp += 1;

        // construct piggy back values on message
        JSONObject jj = new JSONObject();
        jj.put(Lingo.M, message);
        jj.put(Lingo.PID, pid);
        jj.put(Lingo.SGP, sgp);
        JSONArray ja = new JSONArray();
        for (Map.Entry entry : rgq.entrySet()) {
            JSONObject jo = new JSONObject();
            jo.put(entry.getKey(), entry.getValue());
            ja.add(jo);
        }
        jj.put(Lingo.ACK, ja);

        byte[] b = jj.toJSONString().getBytes();
        _mcast(b);

        /*
         In the book, it said:
          " The multicaster p then IP-multicasts the message with its piggybacked
            values to g, and increments sgp by one."

         In that case, sgp has to initialize with one. Otherwise we will have awkward
         situation such that when checking look ahead sequence number S and, S = 0 + 1 = 1
         but while we expect next sequence number S is equal 2!

         If you like to test this awkward condition, go ahead and modify sgp = 0L, and run the program.
        */
        sgp += 1;
    }

    private void _rr(String qid, Long delta) {
        // requestRetransmit
        System.out.println("request retransmit");

        JSONObject jj = new JSONObject();
        JSONArray ja = new JSONArray();
        // requests retransmit through negative ack
        for (int i=1; i <= delta; i++) {
            JSONObject jo = new JSONObject();
            jo.put(qid, i);
            ja.add(jo);
        }
        jj.put(Lingo.NACK, ja);

        byte[] b = jj.toJSONString().getBytes();
        _mcast(b);
    }

    private void _mcast(byte[] bytes) {
        byte[] b = bytes;

        DatagramPacket datagram = new DatagramPacket(b, b.length, group, port);
        System.out.println("Process [" + this.pid + "] [SENDING] " + b.length + " bytes to group " + datagram.getAddress() + ':' + datagram.getPort());

        try {
            socket.send(datagram);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void run() {

        System.out.println(String.format("Starting multicast server process: [%s]", this.pid));

        while (true) {
            try {

                byte[] buffer = new byte[RCV_BUF];
                DatagramPacket datagram = new DatagramPacket(buffer, buffer.length);

                socket.receive(datagram);

                String msg = new String(datagram.getData()).trim();

                JSONObject jj = (JSONObject) parser.parse(msg);
                String m = (String) jj.get(Lingo.M.toString());
                String qid = (String) jj.get(Lingo.PID.toString());
                Long S = (Long) jj.get(Lingo.SGP.toString());

                // is it first time or the expected next one sequence number from q then deliver
                if (rgp.get(qid) == null || (S == rgp.get(qid) + 1)) {

                    // always deliver for now, i.e. assume no issue delivering
                    boolean deliver = true;
                    // print to console to take as delivery message to upper layer
                    System.out.println(String.format("Process [%s] [DELIVER] [%s] - %s", this.pid, ts(), msg));

                    if (deliver) {
                        //  Each process also records Rgq, the sequence number of the latest message
                        // it has delivered from process q that was sent to group g.
                        rgq.put(qid, S);

                        // increment rgp immediately after delivery
                        if (rgp.get(qid) == null) {
                            // this is due to Map data structure, initially it is null, rather than 0.
                            // this is to take as 0, as in Vector structure counterpart.
                            rgp.put(qid, S);
                            // of course, this has rightfully to be  "rgp.put(qid, rgp.get(qid) + 1);"
                            // as else condition below. but due to Map data structure,
                            // rgp.get(qid) call will throws null, rather than initialization condition 0
                            // so, 0 + 1 = 1 i.e. equivalent of initial S
                            // this only happens for first time
                        } else {
                            rgp.put(qid, rgp.get(qid) + 1);
                        }

                    } else {
                        // put in hold-back-queue
                        holdBackQueue.add(msg);
                    }
                }

                else if (S <= rgp.get(qid)) {
                    //  If S ≤ Rgp then message is already delivered, discard
                    System.out.println(String.format("[%s] [%s]  Already delivered, discard.", this.pid, ts()));
                }

                if (S > rgp.get(qid) + 1) {
                    holdBackQueue.add(msg);
                    // request the diff from pid
                    Long delta = S - rgp.get(qid) + 1;
                    _rr(qid, delta);
                }

                JSONArray ack = (JSONArray) jj.get(Lingo.ACK.toString());
                for (Object obj : ack) {
                    JSONObject jo = (JSONObject) obj;
                    for (Object entry : jo.keySet()) {
                        String id = (String) entry;
                        Long R = (Long) jo.get(id);
                        if (R > rgq.get(id)) {
                            holdBackQueue.add(msg); // do we need to put in queue? it didn't say clearly
                            Long delta = R - rgq.get(id);
                            _rr(qid, delta);
                        }
                    }
                }

                // TODO handle NACK, i.e. Process has to the store messages indefinitely
                // Second, the agreement property requires that there is always an available copy of
                // any message needed by a process that did not receive it. We therefore assume that
                // processes retain copies of the messages they have delivered – indefinitely, in
                // this simplified protocol.

                // TODO when to clear hold-back-queue
                // The hold-back queue is not strictly necessary for reliability, but it simplifies the
                // protocol by enabling us to use sequence numbers to represent sets of delivered
                // messages. It also provides us with a guarantee of delivery order (see Section 15.4.3).

                // TODO when to introduce delivery-queue

            } catch (IOException | ParseException e) {
                e.printStackTrace();
            }
        }
    }

    public static String ts() {
        LocalDateTime ldt = LocalDateTime.ofInstant(Instant.now(), ZoneId.systemDefault());
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("hh:mm:ss");
        return ldt.format(dateTimeFormatter);
    }

    public void stop() throws IOException {
        socket.leaveGroup(this.group);
        socket.close();
    }
}
