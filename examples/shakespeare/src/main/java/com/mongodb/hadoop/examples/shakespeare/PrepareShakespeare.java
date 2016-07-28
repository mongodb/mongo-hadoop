package com.mongodb.hadoop.examples.shakespeare;

import com.mongodb.DB;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.gridfs.GridFS;
import com.mongodb.gridfs.GridFSInputFile;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Scanner;
import java.util.regex.Pattern;

/**
 * A tool that splits Shakepeare's complete works into separate files
 * and uploads each to GridFS.
 */
public class PrepareShakespeare implements Tool {
    private Configuration conf;

    public PrepareShakespeare() {
        conf = new Configuration();
    }

    private void printUsage() {
        // CHECKSTYLE:OFF
        System.err.println(
          "USAGE: hadoop jar mongo-hadoop-shakespeare.jar "
            + getClass().getName()
            + " <inputFile> <connection-string with database>");
        // CHECKSTYLE:ON
    }

    @Override
    public int run(final String[] args) throws Exception {
        if (args.length < 2) {
            printUsage();
            return 1;
        }
        String inputFilePath = args[0];
        String mongoURI = args[1];

        MongoClientURI uri = new MongoClientURI(mongoURI);
        MongoClient client = new MongoClient(uri);
        DB gridfsDB = client.getDB(uri.getDatabase());
        GridFS gridFS = new GridFS(gridfsDB);
        Scanner scanner = new Scanner(new File(inputFilePath));
        // Each work is dated with a year.
        Pattern delimiter = Pattern.compile("^\\d{4}", Pattern.MULTILINE);
        scanner.useDelimiter(delimiter);
        int numWorks = 0;
        // Drop database before uploading anything.
        gridfsDB.dropDatabase();
        try {
            for (; scanner.hasNext(); ++numWorks) {
                String nextWork = scanner.next();
                // Skip legal notice/intro.
                if (0 == numWorks) {
                    continue;
                }

                Scanner titleScanner = new Scanner(nextWork);
                String workTitle = null;
                while (titleScanner.hasNextLine()) {
                    String line = titleScanner.nextLine();
                    if (!line.isEmpty()) {
                        // Work title is first non-blank line.
                        workTitle = line;
                        break;
                    }
                }
                if (null == workTitle) {
                    throw new IOException("Could not find a title!");
                }
                GridFSInputFile file = gridFS.createFile(workTitle);
                // Set chunk size low enough that we get multiple chunks.
                file.setChunkSize(1024 * 10);
                OutputStream os = file.getOutputStream();
                os.write(nextWork.getBytes());
                os.close();
            }
        } finally {
            scanner.close();
            client.close();
        }
        System.out.printf("Wrote %d works to GridFS.\n", numWorks);

        return 0;
    }

    @Override
    public void setConf(final Configuration conf) {
        this.conf = conf;
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    public static void main(final String[] args) throws Exception {
        System.exit(ToolRunner.run(new PrepareShakespeare(), args));
    }
}
