package ExtMemMergeSort;

import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

public class FileReader
{
    private final static Logger log = LogManager.getLogger(ExtMemMergeSort.FileReader.class);

    private final List<String> allFiles;

    private static final String DELIMITER = ";";
    private static final String NEW_LINE_SEPARATOR = "\n";

    public FileReader(String directory)
    {
        this.allFiles = allFilesFromDirectory(directory);
    }

    /**
     * Runs an experiment with specified parameters
     *
     * @param totalNrIntegers, nrStreams, nrLoops, streamType, bufferSize
     * @return captured statistics (run time for each trial + average value)
     * @throws IOException if something goes wrong with reading from disk
     */
    public void ReadFile(String streamType, int nrStreams, int totalNrIntegers, int nrLoops, int bufferSize, java.io.FileWriter resultsFile) throws IOException
    {
        for (int count = 0; count < nrLoops; count++)
        {
            List<ReadStream> readers = OpenStreams(streamType, nrStreams, totalNrIntegers, nrLoops, bufferSize);

            StopWatch watch = new StopWatch();
            watch.start();

            ReadFilesSequentially(readers);
            CloseStreams(readers);
            watch.stop();

            log.info("Files read finished. Stream type: {}, Number of streams: {}, Number of integers: {}, Buffer size: {}, Loop: {}, Time: {}, Nano time: {}",
                    streamType, nrStreams, totalNrIntegers, bufferSize, count+1, watch.getTime(), watch.getNanoTime());

            WriteCsvFile(resultsFile, streamType, nrStreams, totalNrIntegers, bufferSize, count+1, String.valueOf(watch.getTime()), String.valueOf(watch.getNanoTime()));
        }
    }

    private List<ReadStream> OpenStreams(String streamType, int nrStreams, int nr, int nrLoops, int bufferSize) throws IOException
    {
        Iterable<String> files = TakeFiles(nrStreams, allFiles);

        List<ReadStream> readers = Lists.newArrayListWithCapacity(nrStreams);
        for (String file : files)
        {
            ReadStream stream = null;
            switch (streamType)
            {
                case "Simple":
                    stream = new ReadStreamSimple();
                    break;
                case "Buffer":
                    stream = new ReadStreamBuffer();
                    break;
                case "BufferB":
                    stream = new ReadStreamBufferB(bufferSize);
                    break;
                case "Mapping":
                    stream = new ReadStreamMapping(bufferSize);
                    break;
                default:
                    stream = new ReadStreamSimple();
            }

            stream.OpenFile(file);
            readers.add(stream);
        }
        return readers;
    }

    private static void ReadFilesSequentially(List<ReadStream> readers) throws IOException
    {
        while (!readers.isEmpty())
        {
            Iterator<ReadStream> readersIterator = readers.iterator();

            while (readersIterator.hasNext())
            {
                ReadStream stream = readersIterator.next();
                if (stream.EndOfStream())
                {
                    readersIterator.remove();
                }
                else
                {
                    stream.ReadNext();
                }
            }
        }
    }

    private static Iterable<String> TakeFiles(int nrStreams, List<String> allFiles)
    {
        ArrayList<String> files = Lists.newArrayList(allFiles);
        //Collections.shuffle(files);
        return Iterables.limit(files, nrStreams);
    }

    private static void CloseStreams(List<ReadStream> readers) throws IOException
    {
        for (ReadStream stream : readers)
        {
            stream.Close();
        }
    }

    private static List<String> allFilesFromDirectory(String directoryName)
    {
        File directory = new File(directoryName);
        File[] files = directory.listFiles();
        Validate.notEmpty(files, "Directory %s must not be empty.", directoryName);

        List<String> result = Lists.newArrayListWithCapacity(files.length);
        for (File file : files)
        {
            result.add(file.getAbsolutePath());
        }
        return result;
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
