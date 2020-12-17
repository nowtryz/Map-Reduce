package net.nowtryz.mapreduce.server;

import lombok.extern.log4j.Log4j2;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CompletionException;

/**
 *
 */
@Log4j2
public class Server {
    private final CoordinatorServer coordinatorServer;
    private final Terminal terminal;
    private final int maxChunkSize;

    public static void start(int port, int maxChunkSize) throws IOException {
        new Server(port, maxChunkSize).init();
    }

    private Server(int port, int maxChunkSize) {
        this.coordinatorServer = new CoordinatorServer(port);
        this.terminal = new Terminal(this);
        this.maxChunkSize = maxChunkSize;
    }

    private void init() throws IOException {
        this.coordinatorServer.startConnection();
        this.terminal.readInputs();

        System.exit(0);
    }

    void stop() {
        log.info("Stopping server");
        this.coordinatorServer.stopAll();
    }

    void computeFile(File file) {
        MapReduceOperation operation = new MapReduceOperation(this.coordinatorServer, file, this.maxChunkSize);

        try {
            Map<String, Integer> counts = operation.compute();

            log.info("=============================================");
            log.info("Compute complete:");
            counts.forEach((s, i) -> log.info(String.format("`%s`: %d", s, i)));
            log.info("=============================================");

        } catch (IOException | CompletionException exception) {
            exception.printStackTrace();
        }
    }
}
