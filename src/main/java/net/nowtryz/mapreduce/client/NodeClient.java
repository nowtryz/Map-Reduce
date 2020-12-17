package net.nowtryz.mapreduce.client;

import lombok.extern.log4j.Log4j2;
import net.nowtryz.mapreduce.functions.Mapper;
import net.nowtryz.mapreduce.functions.Reducer;
import net.nowtryz.mapreduce.packets.*;
import static net.nowtryz.mapreduce.utils.HostUtil.getHostName;
import static net.nowtryz.mapreduce.utils.HostUtil.getCpuNumber;
import static net.nowtryz.mapreduce.utils.HostUtil.getRamNumber;

import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.net.SocketException;
import java.util.Map;

@Log4j2
public class NodeClient extends Thread {
    private final String ip;
    private final int port;
    private boolean running = true;
    private Socket clientSocket;
    private ObjectOutputStream out;
    private ObjectInputStream in;

    public NodeClient(String ip, int port) {
        this.ip = ip;
        this.port = port;
        this.setName("Client");
    }

    @Override
    public void run() {
        log.info("Starting client");
        do {
            try {
                log.info("Initializing connection");
                this.init();
                this.listen();
            } catch (IOException exception) {
                log.error("Cannot connect to coordinator: " + exception.getMessage());
            }

            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                log.error("Thread interrupted", e);
            }

            if (running) log.info("Reconnecting...");
        } while (running);

        if (!this.clientSocket.isClosed()) try {
            this.stopConnection();
        } catch (Exception exception) {
            log.error("Unable to disconnect from server ");
            log.debug("Caused by:", exception);
        }
    }

    private void init() throws IOException {
        this.clientSocket = new Socket(this.ip, this.port);
        log.info("Connected to coordinator");

        this.out = new ObjectOutputStream(this.clientSocket.getOutputStream());
        this.in = new ObjectInputStream(this.clientSocket.getInputStream());
        //envoyer le helloPacket
        HelloPacket helloPacket = new HelloPacket(getHostName(),getCpuNumber(),getRamNumber());
        this.out.writeObject(helloPacket);
        
    }

    private void listen() {
        try {
            while (this.running) {
                log.debug("Waiting for new event from coordinator");
                this.read();
            }
        } catch (SocketException exception) {
            log.warn("Lost connection: " + exception.getMessage());
        } catch (EOFException exception) {
            log.info("Server closed connection");
            this.running = false;
        } catch (IOException exception) {
            log.error("An unexpected exception occurred: ", exception);
        }

    }

    private void read() throws IOException {
        try {
            Packet packet = (Packet) this.in.readObject();
            log.debug("Received packet from coordinator");

            if (ShutdownPacket.class.equals(packet.getClass())) {
                this.running = false;
                log.info("Shutdown Packet received, exiting");
                //noinspection UnnecessaryReturnStatement
                return;
            } else if (MapPacket.class.equals(packet.getClass())) {
                this.map((MapPacket) packet);
            } else if (ReducePacket.class.equals(packet.getClass())){
                this.reduce((ReducePacket) packet);
            }

        } catch (ClassNotFoundException | ClassCastException exception) {
            log.error("The received message was not a Packet: " + exception.getMessage());
        }
    }

    private void map(MapPacket packet) throws IOException {
        log.trace("Starting mapping...");
        Map<String, Integer> result = Mapper.countWords(Mapper.explodeLine(packet.getLine()));

        log.debug("Mapped line, sending result...");
        this.out.writeObject(new MapResultPacket(packet.getRequestId(), result));
        this.out.flush();
        log.trace("Sent: " + result);
    }

    private void reduce(ReducePacket packet) throws IOException {
        log.trace("Starting reduction...");
        Map<String, Integer> mapData = Reducer.reduce(packet.getData());

        log.debug("Reduced chunk, sending result...");
        this.out.writeObject(new ReduceResultPacket(packet.getRequestId(), mapData));
        this.out.flush();
        log.trace("Sent: " + mapData);
    }

    public void stopConnection() throws IOException {
        this.in.close();
        this.out.close();
        this.clientSocket.close();
    }
}
