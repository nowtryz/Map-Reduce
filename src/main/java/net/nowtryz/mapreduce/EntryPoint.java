package net.nowtryz.mapreduce;

import lombok.extern.log4j.Log4j2;
import net.nowtryz.mapreduce.client.NodeClient;
import net.nowtryz.mapreduce.server.CoordinatorServer;
import net.nowtryz.mapreduce.server.MapReduceOperation;
import net.nowtryz.mapreduce.server.Server;
import org.apache.commons.cli.*;

import java.io.IOException;
import java.util.Optional;

@Log4j2
public class EntryPoint {
    private static final String USAGE = "You must specify the mode: either `server`or `node`";
    private static final HelpFormatter formatter = new HelpFormatter();
    private static final Options options = new Options()
            .addOption("h", "help", false, "Show this help message")
            .addOption("S", "server-mode", false, "sets the node's moder to server")
            .addOption(Option.builder("C")
                    .longOpt("max-chunk-size")
                    .desc("In server mode, limit chunks to SIZE")
                    .hasArg()
                    .valueSeparator()
                    .argName("SIZE")
                    .type(PatternOptionBuilder.NUMBER_VALUE)
                    .build())
            .addOption(Option.builder("H")
                    .longOpt("host")
                    .desc("In client mode, server node's ip address to connect to")
                    .hasArg()
                    .argName("HOST")
                    .build())
            .addOption(Option.builder("P")
                    .longOpt("port")
                    .desc("In client mode, server node's port to connect to.\nIn server mode, port to listen to")
                    .hasArg()
                    .argName("PORT")
                    .type(PatternOptionBuilder.NUMBER_VALUE)
                    .build());


    public static void main(String[] args) {
        CommandLineParser parser = new DefaultParser();

        try {
            // parse the command line arguments
            CommandLine line = parser.parse( options, args );
            if (line.hasOption('h')) showHelp();
            else start(line);

        } catch(ParseException exception) {
            // oops, something went wrong
            System.err.println( "Parsing failed.  Reason: " + exception.getMessage() );
            showHelp();
        }
    }

    private static void showHelp() {
        // automatically generate the help statement
        formatter.printHelp( "map-reduce", options );
    }

    private static void start(CommandLine commandLine) throws ParseException {
        int maxChunkSize = Optional.ofNullable(commandLine.getParsedOptionValue("max-chunk-size"))
                .map(Number.class::cast)
                .map(Number::intValue)
                .orElse(MapReduceOperation.DEFAULT_MAX_CHUNK_SIZE);
        int port = Optional.ofNullable(commandLine.getParsedOptionValue("port"))
                .map(Number.class::cast)
                .map(Number::intValue)
                .orElse(CoordinatorServer.DEFAULT_PORT);
        String host = Optional.ofNullable(commandLine.getOptionValue("host"))
                .orElse("localhost");

        try {
            if (commandLine.hasOption('S')) Server.start(port, maxChunkSize);
            else new NodeClient(host, port).start();
        } catch (IOException exception) {
            log.fatal("Unable to start the program: " + exception.getMessage());
        }
    }
}
