package net.nowtryz.mapreduce;

import lombok.extern.log4j.Log4j2;
import net.nowtryz.mapreduce.client.NodeClient;
import net.nowtryz.mapreduce.server.CoordinatorServer;
import net.nowtryz.mapreduce.server.Server;

import java.io.IOException;

@Log4j2
public class EntryPoint {
    private static final String USAGE = "You must specify the mode: either `server`or `node`";
    public static void main(String[] args) {
        if (args.length == 0) {
            log.warn(USAGE);
        } else try {
            switch (args[0].toLowerCase()) {
                case "node":
                    new NodeClient("localhost", CoordinatorServer.DEFAULT_PORT).start();
                    break;
                case "server":
                    Server.start(CoordinatorServer.DEFAULT_PORT);
                    break;
                default:
                    log.warn(USAGE);
            }
        } catch (IOException exception) {
            log.fatal("Unable to start the program: " + exception.getMessage());
        }
    }
}
