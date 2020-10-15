package net.nowtryz.mapreduce.server;

import lombok.SneakyThrows;
import lombok.extern.log4j.Log4j2;
import net.nowtryz.mapreduce.packets.*;
import net.nowtryz.mapreduce.request.Request;
import net.nowtryz.mapreduce.request.Response;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;

@Log4j2
public class CoordinatorServer {
    public static final int DEFAULT_PORT = 1667;
    private final List<NodeDaemon> clients = Collections.synchronizedList(new ArrayList<>());
    private final BlockingQueue<NodeDaemon> availableNodes = new LinkedBlockingQueue<>();
    private final int port;

    private boolean running = true;

    private ServerSocket serverSocket;

    public CoordinatorServer(int port) {
        this.port = port;
    }

    public void startConnection() throws IOException {
        this.serverSocket = new ServerSocket(port);
        new Thread(this::listen, "Coordinator-Server").start();
    }

    @SneakyThrows
    private void listen() {
        log.info(String.format("Server listening on port `%d`", this.port));
        while (this.running) {
            log.info("Waiting for new nodes...");
            Socket socket = null;

            try {
                socket = serverSocket.accept();
                NodeDaemon client = new NodeDaemon(this, socket);
                this.clients.add(client);
                client.start();

                log.info("Current pool size is " + this.clients.size());
                this.availableNodes.add(client);
            } catch (IOException exception) {
                log.error("Unable to handle client connection", exception);
                if (socket != null && !socket.isClosed()) socket.close();
            }
        }
    }

    public void stopAll() {
        for (NodeDaemon client : this.clients) {
            try {
                client.stop();
            } catch (Exception exception) {
                log.error("Unable to disconnect client " + client.getUuid());
                log.debug("Caused by:", exception);
            }
        }
    }

    public CompletableFuture<Map<String, Integer>> startMapping(String line) {
        CompletableFuture<Map<String, Integer>> future = this.request(new MapPacket(line))
                .getFuture()
                .thenApply(Response::getReceivedPacket)
                .thenApply(MapResultPacket.class::cast)
                .thenApply(MapResultPacket::getResult);

        future.thenAccept(map -> log.trace("Result is: " + map));
        return future;
    }

    public CompletableFuture<Map<String, Integer>> startReduce(Map<String, List<Integer>> mapList) {
        CompletableFuture<Map<String, Integer>> future = this.request(new ReducePacket(mapList))
                .getFuture()
                .thenApply(Response::getReceivedPacket)
                .thenApply(ReduceResultPacket.class::cast)
                .thenApply(ReduceResultPacket::getResult);

        future.thenAccept(map -> log.trace("Reduce is: " + map));
        return future;
    }

    private Request request(Packet.RequestPacket packet) {
        Request request = new Request(packet);
        this.findNodeAndSendAsync(request);
        return request;
    }

    private void findNodeAndSendAsync(Request request) {
        new Thread(() -> this.findNodeAndSend(request), "NodeFinder-" + request.getRequestId()).start();
    }

    private void findNodeAndSend(Request request) {
        try {
            if (this.availableNodes.isEmpty()) log.debug(String.format(
                    "Request %s queued, waiting for available nodes",
                    request.getRequestId())
            );

            // Block thread until a node is available
            NodeDaemon node = this.availableNodes.take();
            // Node take the request from here
            node.sendRequest(request);
        } catch (IOException e) {
            log.error("An error occurred while sending the request " + request.getRequestId(), e);
            log.error("Lost a packet");
            request.getFuture().completeExceptionally(e);
        } catch (InterruptedException e) {
            log.error("Unable to find a node to send the request " + request.getRequestId() + ": " + e.getMessage());
            log.error("Lost a packet");
            request.getFuture().completeExceptionally(e);
        }
    }

    public void nodeReady(NodeDaemon node) {
        log.debug("released one node");
        this.availableNodes.add(node);
    }

    public void disconnect(NodeDaemon client) {
        this.availableNodes.remove(client);
        this.clients.remove(client);
        client.closeConnection();

        if (client.isBusy()) client.getRequests().forEach(this::findNodeAndSendAsync);
    }

    public int getPoolSize(){
        return this.clients.size();
    }
}
