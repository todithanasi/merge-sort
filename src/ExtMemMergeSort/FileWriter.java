package ExtMemMergeSort;

import org.apache.commons.lang3.Validate;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.time.StopWatch;


public class FileWriter
{
    private final static Logger log = LogManager.getLogger(ExtMemMergeSort.FileWriter.class);
    private final String directoryName;
    private final boolean keepFiles;
    //Delimiter used in CSV file
    private static final String DELIMITER = ";";
    private static final String NEW_LINE_SEPARATOR = "\n";

    /**
     * Constructor to initialize the file writer class.
     *
     * @param directoryName Name of the directory to write files to.
     * @param keepFiles     If set to true the files will not be deleted from the specified directory.
     */
    public FileWriter(String directoryName, boolean keepFiles)
    {
        this.directoryName = directoryName;
        this.keepFiles = keepFiles;
    }

    /**
     * This method writes N random integers to each of the k streams.
     *
     * @param totalNrIntegers, nrStreams, nrLoops, streamType, bufferSize.
     * @throws IOException if there is an unhandled exception during file write.
     */
    public void WriteStreamsInFile(String streamType, int nrStreams, int totalNrIntegers, int nrLoops, int bufferSize, java.io.FileWriter resultsFile) throws IOException
    {
        CheckDirectory(directoryName);

        // Loop N times.
        for (int count = 0; count < nrLoops; count++)
        {
            WriteStream[] streams = new WriteStream[nrStreams];

            for (int i = 0; i < streams.length; i++)
            {
                switch (streamType)
                {
                    case "Simple":
                        streams[i] = new WriteStreamSimple();
                        break;
                    case "Buffer":
                        streams[i] = new WriteStreamBuffer();
                        break;
                    case "BufferB":
                        streams[i] = new WriteStreamBufferB(bufferSize);
                        break;
                    case "Mapping":
                        streams[i] = new WriteStreamMapping(bufferSize);
                        break;
                    default:
                        streams[i] = new WriteStreamSimple();
                }
                streams[i].CreateFile(directoryName + "/" + totalNrIntegers + "_" + UUID.randomUUID().toString() + ".dat", false);
            }

            try
            {
                StopWatch watch = new StopWatch();
                watch.start();
                WriteFilesSequentially(streams, totalNrIntegers);
                CloseStreams(streams);
                watch.stop();
                log.info("Files write finished. Stream type: {}, Number of streams: {}, Number of integers: {}, Buffer size: {}, Loop: {}, Time: {}, Nano time: {}",
                        streamType, nrStreams, totalNrIntegers, bufferSize, count+1, watch.getTime(), watch.getNanoTime());

                WriteCsvFile(resultsFile, streamType, nrStreams, totalNrIntegers, bufferSize, count+1, String.valueOf(watch.getTime()), String.valueOf(watch.getNanoTime()));
            } catch (IOException ex)
            {
                log.error("Parameters: Stream type: {}, Number of streams: {}, Number of integers: {}, Buffer size: {}, Loop: {}. ------ Exception: {}", streamType, nrStreams, totalNrIntegers, bufferSize, count+1, ex);
            }
        }

        if (!keepFiles)
        {
            CleanDirectory(directoryName);
        }
    }

    private static void WriteFilesSequentially(WriteStream[] streams, int totalNrIntegers) throws IOException
    {
        // Write one random integer at once to streams one by one.

        Random randomInt = new Random();

        for (int i = 0; i < totalNrIntegers; i++)
        {
            for (WriteStream ws : streams)
            {
                int randNum = randomInt.nextInt();
                ws.WriteElement(randNum);
            }
        }
    }

    private static void CloseStreams(WriteStream[] streams) throws IOException
    {
        for (WriteStream ws : streams)
        {
            ws.Close();
        }
    }

    private static void CheckDirectory(String directory)
    {
        File file = new File(directory);
        if (!file.exists())
        {
            file.mkdirs();
        }
    }

    public static void CleanDirectory(String deleteDirectory)
    {
        try
        {
            File directory = new File(deleteDirectory);
            if (directory.isDirectory())
            {
                File[] files = directory.listFiles();
                if (files != null && files.length > 0)
                {
                    for (File file : files)
                    {
                        RemoveDirectory(file);
                    }
                }
            }
        } catch (Exception ex)
        {
            //log.error(ex);
        }
    }
    private static void RemoveDirectory(File directory)
    {
        try
        {
            System.gc();
            if (directory.isDirectory())
            {

                File[] files = directory.listFiles();
                if (files != null && files.length > 0)
                {
                    for (File file : files)
                    {
                        RemoveDirectory(file);
                    }
                }
                Path filePath = Paths.get(directory.getPath());

                Files.delete(filePath);

            } else
            {
                Path filePath = Paths.get(directory.getPath());

                Files.delete(filePath);

            }
        } catch (Exception ex)
        {
            //log.error(ex);
        }
    }

    public static void WriteCsvFile(java.io.FileWriter fileWriter, String streamType, int nrStreams, int totalNrIntegers, int bufferSize, int nrLoops, String time, String nanoTime)
    {
        try
        {
            fileWriter.append(streamType);
            fileWriter.append(DELIMITER);
            fileWriter.append(String.valueOf(nrStreams));
            fileWriter.append(DELIMITER);
            fileWriter.append(String.valueOf(totalNrIntegers));
            fileWriter.append(DELIMITER);
            fileWriter.append(String.valueOf(bufferSize));
            fileWriter.append(DELIMITER);
            fileWriter.append(String.valueOf(nrLoops));
            fileWriter.append(DELIMITER);
            fileWriter.append(time);
            fileWriter.append(DELIMITER);
            fileWriter.append(nanoTime);
            fileWriter.append(NEW_LINE_SEPARATOR);

        }
        catch (Exception ex)
        {
            log.error(ex);

        }
    }
}
