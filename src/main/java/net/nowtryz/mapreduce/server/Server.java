package net.nowtryz.mapreduce.server;

import lombok.extern.log4j.Log4j2;
import net.nowtryz.mapreduce.Coordinator;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.text.CharacterIterator;
import java.text.StringCharacterIterator;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

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
    }

    void computeFile(File file) {
        String size = humanReadableByteCountBin(file.length());
        log.info(String.format("\"Starting computing of %s (%s)", file.getName(), size));

        try (Stream<String> stream = Files.lines(file.toPath())) {
            Map<String, AtomicInteger> counts = new ConcurrentHashMap<>();

            stream
                    .parallel()
                    .map(this.coordinatorServer::startMapping)
                    .map(CompletableFuture::join)
                    .forEach(map -> map.forEach((key, value) -> counts.compute(key, (s, atomicInteger) -> {
                        if (atomicInteger == null) return new AtomicInteger(value);
                        atomicInteger.addAndGet(value);
                        return atomicInteger;
                    })));

            log.info("=============================================");
            log.info("Compute complete:");
            counts.forEach((s, i) -> log.info(String.format("`%s`: %d", s, i.get())));
            log.info("=============================================");

        } catch (IOException exception) {
            exception.printStackTrace();
        }
    }

    /**
     *
     * @see <a href="https://stackoverflow.com/questions/3758606/how-can-i-convert-byte-size-into-a-human-readable-format-in-java">On Stackoverflow</a>
     * @param bytes the file size
     * @return the file size in a human readable format
     */
    public static String humanReadableByteCountBin(long bytes) {
        long absB = bytes == Long.MIN_VALUE ? Long.MAX_VALUE : Math.abs(bytes);
        if (absB < 1024) {
            return bytes + " B";
        }
        long value = absB;
        CharacterIterator ci = new StringCharacterIterator("KMGTPE");
        for (int i = 40; i >= 0 && absB > 0xfffccccccccccccL >> i; i -= 10) {
            value >>= 10;
            ci.next();
        }
        value *= Long.signum(bytes);
        return String.format("%.1f %ciB", value / 1024.0, ci.current());
    }
}
