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

    public static void start(int port) throws IOException {
        new Server(port).init();
    }

    private Server(int port) {
        this.coordinatorServer = new CoordinatorServer(port);
        this.terminal = new Terminal(this);
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
        MapReduceOperation operation = new MapReduceOperation(this.coordinatorServer, file);

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
