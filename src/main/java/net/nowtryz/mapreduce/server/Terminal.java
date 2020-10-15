package net.nowtryz.mapreduce.server;

import lombok.extern.log4j.Log4j2;

import java.io.File;
import java.util.Scanner;

@Log4j2
public class Terminal {
    private final Scanner scanner = new Scanner(System.in);
    private final Server server;

    public Terminal(Server server) {
        this.server = server;
    }

    public void readInputs() {
        while (true) {
            String filename = scanner.nextLine();

            if (filename == null || filename.equalsIgnoreCase("stop") || filename.equalsIgnoreCase("exit")) {
                this.server.stop();
                break;
            }

            File file = new File(filename);
            if (!file.exists()) log.error(String.format("`%s` does not exist", filename));
            else if (file.isDirectory()) log.error(String.format("`%s` is a directory", filename));
            else if (!file.canRead()) log.error(String.format("Cannot open `%s`", filename));
            else this.server.computeFile(file);
        }
    }
}
