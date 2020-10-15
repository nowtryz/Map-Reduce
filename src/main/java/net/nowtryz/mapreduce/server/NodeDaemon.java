package net.nowtryz.mapreduce.server;

import lombok.Getter;
import lombok.extern.log4j.Log4j2;
import net.nowtryz.mapreduce.packets.Packet;
import net.nowtryz.mapreduce.packets.Packet.ResultPacket;
import net.nowtryz.mapreduce.packets.ShutdownPacket;
import net.nowtryz.mapreduce.request.Request;
import net.nowtryz.mapreduce.request.Response;

import java.io.Closeable;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

@Log4j2
public class NodeDaemon {
    private static final AtomicInteger idCounter = new AtomicInteger(0);

    @Getter
    private final UUID uuid = UUID.randomUUID();
    private final int id = idCounter.incrementAndGet();
    private final Socket socket;
    private final CoordinatorServer server;

    private final Map<UUID, Request> requests = new HashMap<>();
    private ObjectOutputStream out;
    private ObjectInputStream in;

    public NodeDaemon(CoordinatorServer server, Socket socket) {
        this.server = server;
        this.socket = socket;
    }

    public void start() throws IOException {
        this.out = new ObjectOutputStream(this.socket.getOutputStream());
        this.in = new ObjectInputStream(this.socket.getInputStream());

        log.info(String.format("Node %s joined the cluster", this.uuid));
        new Thread(this::listen, "NodeDaemon-" + this.id).start();

    }

    private void listen() {
        log.info("Listening");
        try {
            while (true) this.read();
        } catch (IOException exception) {
            this.server.disconnect(this);
            log.warn(String.format("Node %s left cluster: %s", this.uuid, exception.getMessage()));
        }
    }

    private void read() throws IOException {
        try {
            ResultPacket packet = (ResultPacket) this.in.readObject();
            log.debug("Received packet from node " + this.uuid);

            new Thread(
                    () -> this.handlePacket(packet),
                    "ResultHandler-" + packet.getRequestId()
            ).start();
        } catch (ClassNotFoundException | ClassCastException exception) {
            log.error("Unknown packet received: " + exception.getMessage());
        }
    }

    private void handlePacket(ResultPacket packet) {
        Request request = this.requests.remove(packet.getRequestId());

        synchronized (this.requests) {
            if (this.requests.isEmpty()) this.server.nodeReady(this);
        }

        if (request == null) log.error("No response was expected from the client");
        else request.getFuture().complete(new Response(packet, request));
    }

    /**
     * Send the packet of the request
     * @param request the request to perform
     * @throws IOException if an exception occurred while sending the packet
     */
    public void sendRequest(Request request) throws IOException {
        this.sendPacket(request.getPacket());
        this.requests.put(request.getRequestId(), request);
    }

    public void stop() throws IOException {
        this.sendPacket(new ShutdownPacket());
        this.closeConnection();
    }

    private void sendPacket(Packet.RequestPacket packet) throws IOException {
        this.out.writeObject(packet);
        this.out.flush();
    }

    public boolean isBusy() {
        return !this.requests.isEmpty();
    }

    public Collection<Request> getRequests() {
        return this.requests.values();
    }

    public void closeConnection() {
        if (!this.socket.isClosed()) {
            this.close(this.in, "Unable to close input stream");
            this.close(this.out, "Unable to close output stream");
            this.close(this.socket, "Unable to close client socket");
        }
    }

    private void close(Closeable closeable, String errorMessage) {
        try {
            closeable.close();
        } catch (IOException exception) {
            log.error(errorMessage, exception);
        }
    }
}
