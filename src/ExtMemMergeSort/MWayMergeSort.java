package ExtMemMergeSort;

import com.google.common.collect.Iterators;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.Validate;
import org.apache.commons.io.LineIterator;

import java.io.*;
import java.io.FileReader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import org.apache.commons.lang3.time.StopWatch;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.file.Files;
import java.util.concurrent.TimeUnit;


public class MWayMergeSort
{
    private final static Logger log = LogManager.getLogger(ExtMemMergeSort.MWayMergeSort.class);
    private final String tempDirectory = "temp";
    private final int bufferSize;
    private final int maxNrOfIntegersInMemory;
    private final int maxNoOfMergeStreams;
    //Delimiter used in CSV file
    private static final String DELIMITER = ";";
    private static final String NEW_LINE_SEPARATOR = "\n";


    //File used to store chunk filenames.
    private String chunksListFilename = getRandomFileName();

    /**
     * Constructor of the class.
     *
     * @param maxNrOfIntegersInMemory Maximum number of integers that can fit in memory.
     * @param maxNrOfMergeStreams     Maximum number of streams that can be merged in one pass.
     * @param bufferSize              Buffer size in bytes for input/output streams (BufferB or Mapping).
     */
    public MWayMergeSort(int maxNrOfIntegersInMemory, int maxNrOfMergeStreams, int bufferSize)
    {
        this.maxNrOfIntegersInMemory = maxNrOfIntegersInMemory;
        this.maxNoOfMergeStreams = maxNrOfMergeStreams;
        this.bufferSize = bufferSize;
    }

    /**
     * Method used to do External-memory merge sorting of the file.
     *
     * @param readStreamType  Stream type to read from the files.
     * @param writeStreamType  Write type to read from the files.
     * @param inputFile  Filename of the input unsorted file.
     * @param outputFile Filename of the output sorted file.
     * @param ASC        If set to true, we sort is ascending order, otherwise is descending.
     * @param nrLoops    Number of loops to be performed in order to provide accurate execution time.
     * @param resultsFile File were are written the results from the method.
     * @throws IOException
     */
    public void Sort(String readStreamType, String writeStreamType, String inputFile, String outputFile, boolean ASC, int nrLoops, java.io.FileWriter resultsFile) throws IOException
    {
        for (int count = 0; count < nrLoops; count++)
        {
            try
            {
                StopWatch watch = new StopWatch();
                watch.start();
                int chunksNr = SortedChunks(readStreamType, writeStreamType, inputFile, ASC);

                //Perform merge passes until we obtain 1 merged file.
                while (chunksNr != 1)
                {
                    chunksNr = MergePass(readStreamType, writeStreamType, ASC);
                }

                // Rename last chunk to output file name.
                RenameChunk(outputFile);
                watch.stop();

                log.info("M-Way Merge-Sort finished. Read stream type: {}, Write stream type: {}, Nr streams: {}, Input file: {}, Output file: {}, ASC: {}, Number of integers in memory: {}, Buffer size: {}, Loop: {}, Time: {}, Nano time: {}",
                        readStreamType, writeStreamType, maxNoOfMergeStreams, inputFile, outputFile, ASC, maxNrOfIntegersInMemory, bufferSize, count + 1, watch.getTime(), watch.getNanoTime());

                WriteCsvFile(resultsFile, readStreamType, writeStreamType, maxNoOfMergeStreams, inputFile, outputFile, ASC, maxNrOfIntegersInMemory, bufferSize, count + 1, String.valueOf(watch.getTime()), String.valueOf(watch.getNanoTime()));

                // Delete temporal folder for storing temporal chunks.
                CleanDirectory(this.tempDirectory);
            } catch (Exception ex)
            {
                log.error("Parameters: Read stream type: {}, Write stream type: {}, Nr streams: {}, Input file: {}, " +
                                "Output file: {}, ASC: {}, Number of integers in memory: {}, Buffer size: {}, Loop: {}. ------ Exception: {}",
                        readStreamType, writeStreamType, maxNoOfMergeStreams, inputFile, outputFile, ASC, maxNrOfIntegersInMemory, bufferSize, count + 1, ex);
            }
        }
    }

    /**
     * Method used to divide input file into chunks that are sorted by another method called within this method.
     *
     * @param readStreamType  Stream type to read from the files.
     * @param writeStreamType  Write type to read from the files.
     * @param inputFile  Filename of the input unsorted file.
     * @param ASC        If set to true, we sort is ascending order, otherwise is descending.
     * @return Number of sorted chunks.
     * @throws IOException
     */
    private int SortedChunks(String readStreamType, String writeStreamType, String inputFile, boolean ASC) throws IOException
    {

        ReadStream input = null;
        switch (readStreamType)
        {
            case "Simple":
                input = new ReadStreamSimple();
                break;
            case "Buffer":
                input = new ReadStreamBuffer();
                break;
            case "BufferB":
                input = new ReadStreamBufferB(bufferSize);
                break;
            case "Mapping":
                input = new ReadStreamMapping(bufferSize);
                break;
            default:
                input = new ReadStreamSimple();
        }
        input.OpenFile(inputFile);
        int chunksNr = 0;

        try (PrintWriter pw = new PrintWriter(chunksListFilename))
        {
            while (true)
            {
                // Read next portion of the file with at most 'maxNoOfIntegersInMemory' integers.
                int[] readIntegers = ReadNextPortion(input, maxNrOfIntegersInMemory);
                if (readIntegers.length == 0)
                {
                    break;
                }

                // Sort in-memory.
                Sort(readIntegers, ASC);

                chunksNr++;
                // Create chunk files.
                String chunkFilename = CreateChunk(writeStreamType, readIntegers);

                // Write the file names in the file in order to use them in the next passes.
                pw.println(chunkFilename);
            }

            pw.flush();

            return chunksNr;
        }
    }

    private static void Sort(int[] readIntegers, boolean ASC)
    {
        Arrays.sort(readIntegers);
        if (!ASC)
        {
            ArrayUtils.reverse(readIntegers);
        }
    }

    /**
     * Method to create a new chunk by writing the array of integers.
     *
     * @param writeStreamType  Write type to read from the files.
     * @param readIntegers Array of integers to write into the chunk.
     * @return Path of the chunk.
     * @throws IOException
     */
    private String CreateChunk(String writeStreamType, int[] readIntegers) throws IOException
    {
        String chunkFilePath = getRandomFileName();
        try
        {
            WriteStream writeStream = null;
            switch (writeStreamType)
            {
                case "Simple":
                    writeStream = new WriteStreamSimple();
                    break;
                case "Buffer":
                    writeStream = new WriteStreamBuffer();
                    break;
                case "BufferB":
                    writeStream = new WriteStreamBufferB(bufferSize);
                    break;
                case "Mapping":
                    writeStream = new WriteStreamMapping(bufferSize);
                    break;
                default:
                    writeStream = new WriteStreamSimple();
            }
            writeStream.CreateFile(chunkFilePath, false);

            for (int i = 0; i < readIntegers.length; i++)
            {
                writeStream.WriteElement(readIntegers[i]);
            }
            //NEW
            writeStream.Close();

            return chunkFilePath;
        } catch (IOException ex)
        {
            log.error("Exception: ", ex);
            return chunkFilePath;
        }
    }


    /**
     * Method used to perform merge of the chunks.
     *
     * @param readStreamType  Write type to read from the files.
     * @param writeStreamType  Write type to read from the files.
     * @param ASC Sorting order.
     * @return Number of remaining chunks.
     * @throws IOException
     */
    private int MergePass(String readStreamType, String writeStreamType, boolean ASC) throws IOException
    {
        String newChunksFilenames = getRandomFileName();

        LineIterator chunks = new LineIterator(new java.io.FileReader(this.chunksListFilename));
        Iterator<List<String>> chunksToMerge = Iterators.partition(chunks, this.maxNoOfMergeStreams);

        int resultChunksNr = 0;
        try (PrintWriter pw = new PrintWriter(newChunksFilenames))
        {
            while (chunksToMerge.hasNext())
            {
                // Read next list of chunks to merge from an external memory. List has at most 'maxNoOfMergeStreams' entries.
                List<String> nextPortion = chunksToMerge.next();

                // Merge chunks into a bigger chunk using priority queue.
                String mergedChunk = MergeChunks(readStreamType, writeStreamType, nextPortion, ASC);
                resultChunksNr++;

                // Write the file names in the file in order to use them in the next passes.
                pw.println(mergedChunk);

                // Delete processed chunks.
                DeleteFile(nextPortion);
            }
        }

        chunks.close();
        DeleteFile(this.chunksListFilename);

        this.chunksListFilename = newChunksFilenames;
        return resultChunksNr;
    }

    /**
     * Method used to merge chunks into a bigger chunk using priority queue.
     *
     * @param readStreamType  Write type to read from the files.
     * @param writeStreamType  Write type to read from the files.
     * @param chunks List of chuncks
     * @param ASC Sorting order
     * @return Filename of the merged chunk.
     * @throws IOException
     */
    private String MergeChunks(String readStreamType, String writeStreamType, List<String> chunks, boolean ASC) throws IOException
    {
        PriorityQueue<ReadStream> queue = QueueAllChunks(readStreamType, chunks, ASC);

        String mergedChunk = getRandomFileName();
        try
        {
            WriteStream writeStream = null;
            switch (writeStreamType)
            {
                case "Simple":
                    writeStream = new WriteStreamSimple();
                    break;
                case "Buffer":
                    writeStream = new WriteStreamBuffer();
                    break;
                case "BufferB":
                    writeStream = new WriteStreamBufferB(bufferSize);
                    break;
                case "Mapping":
                    writeStream = new WriteStreamMapping(bufferSize);
                    break;
                default:
                    writeStream = new WriteStreamSimple();
            }
            writeStream.CreateFile(mergedChunk, false);

            while (!queue.isEmpty())
            {
                ReadStream minChunk = queue.poll();
                int minInt = minChunk.ReadNext();

                writeStream.WriteElement(minInt);

                if (!minChunk.EndOfStream())
                {
                    queue.add(minChunk);
                } else
                {
                    minChunk.Close();
                }
            }

        } catch (IOException ex)
        {
            log.error("Exception: ", ex);
        }

        return mergedChunk;
    }


    /**
     * Method used to put chunks in a priority queue in order to use them in the right order
     * during the merge pass.
     *
     * @param readStreamType  Write type to read from the files.
     * @param chunks List of chunks.
     * @param ASC Sorting order.
     * @return Filename of the merged chunk.
     * @throws IOException
     */
    private PriorityQueue<ReadStream> QueueAllChunks(String readStreamType, List<String> chunks, boolean ASC)
            throws IOException
    {
        PriorityQueue<ReadStream> queue = CreatePriorityQueue(ASC);

        for (String nextChunk : chunks)
        {
            ReadStream chunkStream = null;
            switch (readStreamType)
            {
                case "Simple":
                    chunkStream = new ReadStreamSimple();
                    break;
                case "Buffer":
                    chunkStream = new ReadStreamBuffer();
                    break;
                case "BufferB":
                    chunkStream = new ReadStreamBufferB(bufferSize);
                    break;
                case "Mapping":
                    chunkStream = new ReadStreamMapping(bufferSize);
                    break;
                default:
                    chunkStream = new ReadStreamSimple();
            }

            chunkStream.OpenFile(nextChunk);

            if (!chunkStream.EndOfStream())
            {
                queue.add(chunkStream);
            }
        }

        return queue;
    }

    private PriorityQueue<ReadStream> CreatePriorityQueue(boolean ASC)
    {
        if (ASC)
        {
            return new PriorityQueue<ReadStream>(maxNoOfMergeStreams, readStreamComparatorASC);
        } else
        {
            return new PriorityQueue<ReadStream>(maxNoOfMergeStreams, readStreamComparatorDESC);
        }
    }


    /**
     * Method to rename the last chunk in the desired filename.
     *
     * @param outputFile
     * @return true if the file is renamed.
     * @throws IOException
     */
    private boolean RenameChunk(String outputFile) throws IOException
    {
        try (BufferedReader bufferedReader = new BufferedReader(new FileReader(this.chunksListFilename)))
        {
            String lastChunk = bufferedReader.readLine();
            File chunk = new File(lastChunk);
            boolean isRenamed = chunk.renameTo(new File(outputFile));
            bufferedReader.close();
            return isRenamed;

        } catch (Exception ex)
        {
            log.error(ex);
            return false;
        }

    }


    /**
     * Method that puts in buffer the integers that should fit in memory.
     *
     */
    private static int[] ReadNextPortion(ReadStream input, int maxNoOfIntegersInMemory) throws IOException
    {
        Validate.isTrue(input.IsOpen(), "The stream must be open.");

        int buffer[] = new int[maxNoOfIntegersInMemory];

        int readNr = 0;
        while (readNr < maxNoOfIntegersInMemory && !input.EndOfStream())
        {
            buffer[readNr] = input.ReadNext();
            readNr++;
        }

        if (readNr < maxNoOfIntegersInMemory)
        {
            buffer = ArrayUtils.subarray(buffer, 0, readNr);
        }

        return buffer;
    }

    /**
     * Method that sorts integers in ascending order.
     */
    private final Comparator<ReadStream> readStreamComparatorASC = new Comparator<ReadStream>()
    {
        @Override
        public int compare(ReadStream a, ReadStream b)
        {
            if (a.ReadNext() < b.ReadNext())
            {
                return -1;
            } else if (a.ReadNext() == b.ReadNext())
            {
                return 0;
            } else
            {
                return 1;
            }
        }
    };

    /**
     * Method that sorts integers in descending order.
     */
    private final Comparator<ReadStream> readStreamComparatorDESC = new Comparator<ReadStream>()
    {
        @Override
        public int compare(ReadStream a, ReadStream b)
        {
            if (a.ReadNext() < b.ReadNext())
            {
                return 1;
            } else if (a.ReadNext() == b.ReadNext())
            {
                return 0;
            } else
            {
                return -1;
            }
        }
    };

    /**
     * Method used to generate random filename.
     *
     * @return Path.
     */
    private String getRandomFileName()
    {
        CreateDirectoryIfNotExists(this.tempDirectory);
        String randomName = UUID.randomUUID().toString();
        return new File(this.tempDirectory, randomName + ".dat").getAbsolutePath();
    }

    /**
     * Method use to create directory if it does not exist.
     *
     * @param directoryName
     */
    private static void CreateDirectoryIfNotExists(String directoryName)
    {
        File file = new File(directoryName);
        if (!file.exists())
        {
            file.mkdirs();
        }
    }

    /**
     * Method used to delete files of directory.
     *
     */

    private static boolean DeleteFile(String filename)
    {
        try
        {
            System.gc();
            Path filePath = Paths.get(filename);

            Files.delete(filePath);
            return true;
        } catch (Exception ex)
        {
            //log.error(ex);
            return false;
        }
    }

    /**
     * Method used to delete files of directory.
     *
     */

    private static boolean DeleteFile(List<String> filenames)
    {
        boolean result = true;
        for (String filename : filenames)
        {
            try
            {
                System.gc();
                Path filePath = Paths.get(filename);

                result = result && Files.deleteIfExists(filePath);
            } catch (Exception ex)
            {
                //log.error(ex);
                return false;
            }
        }
        return result;
    }

    /**
     * Method used to delete files of directory.
     *
     */
    private static void CleanDirectory(String deleteDirectory)
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

    /**
     * Method used to delete files of directory.
     *
     */
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

    /**
     * Method used to write parameters and results in the csv file.
     *
     *
     */

    public static void WriteCsvFile(java.io.FileWriter fileWriter, String readStreamType, String writeStreamType, int nrStreams, String inputFile, String outputFile, boolean ASC, int maxNrOfIntegersInMemory, int bufferSize, int nrLoops, String time, String nanoTime)
    {
        try
        {
            fileWriter.append(readStreamType);
            fileWriter.append(DELIMITER);
            fileWriter.append(writeStreamType);
            fileWriter.append(DELIMITER);
            fileWriter.append(String.valueOf(nrStreams));
            fileWriter.append(DELIMITER);
            fileWriter.append(inputFile);
            fileWriter.append(DELIMITER);
            fileWriter.append(outputFile);
            fileWriter.append(DELIMITER);
            fileWriter.append(String.valueOf(ASC));
            fileWriter.append(DELIMITER);
            fileWriter.append(String.valueOf(maxNrOfIntegersInMemory));
            fileWriter.append(DELIMITER);
            fileWriter.append(String.valueOf(bufferSize));
            fileWriter.append(DELIMITER);
            fileWriter.append(String.valueOf(nrLoops));
            fileWriter.append(DELIMITER);
            fileWriter.append(time);
            fileWriter.append(DELIMITER);
            fileWriter.append(nanoTime);
            fileWriter.append(NEW_LINE_SEPARATOR);
        } catch (Exception ex)
        {
            log.error(ex);
        }
    }
}
