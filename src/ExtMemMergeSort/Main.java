package ExtMemMergeSort;

import org.apache.logging.log4j.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;


public class Main
{
    private final static Logger log = LogManager.getLogger(ExtMemMergeSort.Main.class);

    private static int nrLoops = 6;
    private static int nrStreams = 30;
    private static String[] streamType = {"Simple", "Buffer", "BufferB", "Mapping"};
    private static Map<Integer, Integer> buffer = new HashMap<Integer, Integer>();

    private static int[] totalNrOfIntegers = new int[]{1000, 10000, 100000, 1000000, 10000000, 100000000, 250000000};

    private static Map<Integer, Integer> bufferMWayMergeSort ;
    private static int[] maxNrOfIntegersInMemory;
    private static int maxNoOfMergeStreams;
    private static String readStreamTypeMWayMergeSort;
    private static String writeStreamTypeMWayMergeSort;
    private static int nrLoopsMWayMergeSort;
    private static String inputFile;
    private static String outputFile;
    private static boolean ASC;

    private static final String DELIMITER = ";";
    private static final String WRITE_NEW_LINE_SEPARATOR = "\n";

    //CSV file header
    private static final String WRITE_FILE_HEADER = "StreamType; NrStreams; TotalNrIntegers; BufferSize; Loop; Time; Nanotime";

    private static final String READ_NEW_LINE_SEPARATOR = "\n";

    //CSV file header
    private static final String READ_FILE_HEADER = "StreamType; NrStreams; TotalNrIntegers; BufferSize; Loop; Time; Nanotime";

    //CSV file header
    private static final String MWAY_FILE_HEADER = "ReadStreamType; WriteStreamType; NrStreams; InputFile; Outputfile; ASC; NumIntegerMemory; BufferSize, Loop, Time, Nanotime";
    private static final String MWAY_NEW_LINE_SEPARATOR = "\n";

    public static void main(String[] args) throws IOException, InterruptedException
    {
        /* Used to find default buffer size.

        FindBufferSize defaultBufferSize = new FindBufferSize(null);
        System.out.println(System.getProperty("java.version"));
        System.out.println(defaultBufferSize.getBufferSize());*/

        SetDefaultValues();

        String selectionMain = "";
        String selectionWrite = "";
        String selectionRead = "";
        String selectionMWayMergeSort = "";
        String selectionWriteParameters = "";
        String selectionReadParameters = "";
        String selectionMWayMergeSortParameters = "";
        ArrayList<Parameters> allParameters = new ArrayList<>();
        do
        {
            selectionWrite = "";
            selectionRead = "";
            selectionMWayMergeSort = "";
            selectionWriteParameters = "";


            System.out.println("[1] Write files.");
            System.out.println("[2] Read files.");
            System.out.println("[3] External memory M-way merge sort.");
            System.out.println("[Q] Quit application");
            CreateBlankLines();
            System.out.println("Select feature value: ");
            Scanner scanner = new Scanner(System.in);
            selectionMain = scanner.nextLine();
            switch (selectionMain)
            {


                // Application handles here the write file function.
                case "1":
                    do
                    {
                        System.out.println("[1] Run write files with default values.");
                        System.out.println("[2] Specify your own values.");
                        System.out.println("[3] Import parameter values from file.");
                        System.out.println("[B] Go back at Main menu.");

                        System.out.println("Select feature value: ");
                        Scanner scannerWrite = new Scanner(System.in);
                        selectionWrite = scannerWrite.nextLine();
                        switch (selectionWrite)
                        {

                            // Run write files with default values.
                            case "1":
                                SetDefaultValues();
                                selectionWrite = "E";
                                break;
                            case "2":

                                // The user specified values for write function are read here. After the user chooses what value to change
                                // we read it and save in the global variables. This process may be repeated several time for each of the
                                // parameters and it is ended when the user decides to execute the write function or goes back at the main menu.
                                do
                                {
                                    SetValueMenu();

                                    Scanner scannerWriteParameters = new Scanner(System.in);
                                    selectionWriteParameters = scannerWriteParameters.nextLine();
                                    switch (selectionWriteParameters)
                                    {
                                        case "1":
                                            System.out.println("Set Stream type: ");
                                            Scanner scannerStreamType = new Scanner(System.in);
                                            try
                                            {
                                                String[] newStreamType = scannerStreamType.nextLine().trim().split(",");
                                                for (int i = 0; i < newStreamType.length; i++)
                                                {
                                                    newStreamType[i] = newStreamType[i].trim();
                                                }
                                                streamType = newStreamType;
                                            } catch (Exception ex)
                                            {
                                                //Handle the error
                                            }

                                            break;
                                        case "2":

                                            System.out.println("Set Number of streams: ");
                                            Scanner scannerNumberOfStreams = new Scanner(System.in);
                                            try
                                            {
                                                String newNumberOfStreams = scannerNumberOfStreams.nextLine().trim();
                                                nrStreams = Integer.parseInt(newNumberOfStreams);
                                            } catch (Exception ex)
                                            {
                                                //Handle the error
                                            }
                                            break;
                                        case "3":
                                            System.out.println("Set Total number of integers: ");
                                            Scanner scannerTotalNrOfIntegers = new Scanner(System.in);
                                            try
                                            {
                                                String[] newTotalNrOfIntegers = scannerTotalNrOfIntegers.nextLine().trim().split(",");
                                                totalNrOfIntegers = new int[newTotalNrOfIntegers.length];
                                                for (int i = 0; i < newTotalNrOfIntegers.length; i++)
                                                {
                                                    totalNrOfIntegers[i] = Integer.parseInt(newTotalNrOfIntegers[i].trim());
                                                }
                                            } catch (Exception ex)
                                            {
                                                log.error(ex);
                                            }
                                            break;
                                        case "4":
                                            System.out.println("Set Buffer Size: ");
                                            Scanner scannerBufferSize = new Scanner(System.in);
                                            try
                                            {
                                                String[] newBufferSize = scannerBufferSize.nextLine().trim().split(",");
                                                buffer = new HashMap<Integer, Integer>();
                                                buffer.put(Integer.parseInt(newBufferSize[0].trim()), Integer.parseInt(newBufferSize[1].trim()));
                                            } catch (Exception ex)
                                            {
                                                //Handle the error
                                            }
                                            break;
                                        case "5":
                                            System.out.println("Set Loops: ");
                                            Scanner scannerLoops = new Scanner(System.in);
                                            try
                                            {
                                                String newLoops = scannerLoops.nextLine().trim();
                                                nrLoops = Integer.parseInt(newLoops);
                                            } catch (Exception ex)
                                            {
                                                //Handle the error
                                            }
                                            break;
                                        case "E":
                                            selectionWrite = "E";
                                            selectionMain = "";
                                            break;
                                        case "B":
                                            break;
                                        default:
                                            System.out.println("The selected value doesn't exist!");
                                    }
                                }
                                while (!selectionWriteParameters.equals("B") && !selectionWriteParameters.equals("E"));

                                break;
                            case "3":

                                // The write function is performed with values defined in the csv file.
                                System.out.println("Set parameters file full path: ");
                                Scanner readFilePath = new Scanner(System.in);
                                String filePath = readFilePath.nextLine();
                                allParameters = ReadWriteCsvFile(filePath);
                                selectionWrite = "E";
                                break;
                            case "B":
                                // Go back at main menu.
                                selectionMain = "";
                                break;
                            default:
                                System.out.println("The selected value doesn't exist!");
                        }
                    } while (!selectionWrite.equals("B") && !selectionWrite.equals("E"));
                    // After specifying the values by one of the above mentioned ways, it is time to execute the
                    // the write function.
                    if (selectionWrite.equals("E"))
                    {
                        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd_HH_mm_ss");
                        Date date = new Date();
                        String datePart = dateFormat.format(date);
                        String resultsWriteFilename = "WriteResults_"+datePart+".csv";

                        java.io.FileWriter fileWriterResults = new java.io.FileWriter(resultsWriteFilename);
                        //Write the CSV file header
                        fileWriterResults.append(WRITE_FILE_HEADER);
                        //Add a new line separator after the header
                        fileWriterResults.append(WRITE_NEW_LINE_SEPARATOR);

                        // The case when parameters are defined in csv files.
                        for (Parameters parameter : allParameters)
                        {
                            if (parameter != null)
                            {
                                nrStreams = parameter.getNrStreams();
                                totalNrOfIntegers = parameter.getTotalNrOfIntegers();
                                streamType = new String[]{parameter.getStreamType()};
                                buffer = parameter.getBuffer();
                                nrLoops = parameter.getNrLoops();
                            }
                                for (int totalNrOfInteger : totalNrOfIntegers)
                                {
                                    for (String type : streamType)
                                    {
                                        if (type.equals("BufferB") || type.equals("Mapping"))
                                        {
                                            Map.Entry<Integer, Integer> entry = buffer.entrySet().iterator().next();
                                            for (int i = entry.getKey(); i <= entry.getValue(); i++)
                                            {
                                                String outputFolderName = "FilesFolder/" + String.valueOf(nrStreams) + '/' + String.valueOf(totalNrOfInteger) + '/';
                                                FileWriter writer = new FileWriter(outputFolderName, true);
                                                int calculatedBuffer = (int) Math.pow((double) 2, (double) i);
                                                writer.WriteStreamsInFile(type, nrStreams, totalNrOfInteger, nrLoops, calculatedBuffer,fileWriterResults);
                                            }
                                        } else
                                        {
                                            String outputFolderName = "FilesFolder/" + String.valueOf(nrStreams) + '/' + String.valueOf(totalNrOfInteger) + '/';
                                            FileWriter writer = new FileWriter(outputFolderName, true);
                                            writer.WriteStreamsInFile(type, nrStreams, totalNrOfInteger, nrLoops, 1, fileWriterResults);
                                        }
                                    }
                                }
                                try
                                {
                                    fileWriterResults.flush();
                                } catch (IOException ex)
                                {
                                    log.error(ex);
                                }
                        }

                        // The case when parameters are set manually by the user or used default values.
                        if(allParameters == null || allParameters.size() == 0)
                        {
                                for (int totalNrOfInteger : totalNrOfIntegers)
                                {
                                    for (String type : streamType)
                                    {
                                        if (type.equals("BufferB") || type.equals("Mapping"))
                                        {
                                            Map.Entry<Integer, Integer> entry = buffer.entrySet().iterator().next();
                                            for (int i = entry.getKey(); i <= entry.getValue(); i++)
                                            {
                                                String outputFolderName = "FilesFolder/" + String.valueOf(nrStreams) + '/' + String.valueOf(totalNrOfInteger) + '/';
                                                FileWriter writer = new FileWriter(outputFolderName, true);
                                                int calculatedBuffer = (int) Math.pow((double) 2, (double) i);
                                                writer.WriteStreamsInFile(type, nrStreams, totalNrOfInteger, nrLoops, calculatedBuffer, fileWriterResults);
                                            }
                                        } else
                                        {
                                            String outputFolderName = "FilesFolder/" + String.valueOf(nrStreams) + '/' + String.valueOf(totalNrOfInteger) + '/';
                                            FileWriter writer = new FileWriter(outputFolderName, true);
                                            writer.WriteStreamsInFile(type, nrStreams, totalNrOfInteger, nrLoops, 1, fileWriterResults);
                                        }
                                    }
                                }
                                try
                                {
                                    fileWriterResults.flush();
                                } catch (IOException ex)
                                {
                                    log.error(ex);
                                }
                        }

                        try
                        {
                            fileWriterResults.flush();
                            fileWriterResults.close();
                        } catch (IOException ex)
                        {
                            log.error(ex);
                        }
                    }
                    break;
                case "2":

                    // Application handles here the read file function.
                    do
                    {
                        System.out.println("[1] Run read files with default values.");
                        System.out.println("[2] Specify your own values.");
                        System.out.println("[3] Import parameter values from file.");
                        System.out.println("[M] Go back at Main menu.");

                        System.out.println("Select feature value: ");
                        Scanner scannerRead = new Scanner(System.in);
                        selectionRead = scannerRead.nextLine();
                        switch (selectionRead)
                        {
                            // Run write files with default values. Normally the write file function should be run before
                            // otherwise files and folder structure to read them will be missing and the function will fail.
                            case "1":
                                SetDefaultValues();
                                selectionRead = "E";
                                break;
                            case "2":

                                // The user values for read function are specified here. After the user chooses what value to change
                                // we read it and save in the global variables. This process may be repeated several time for each of the
                                // parameters and it is ended when the user decides to execute the read function or goes back at the main menu.

                                do
                                {
                                    SetValueMenu();

                                    Scanner scannerReadParameters = new Scanner(System.in);
                                    selectionReadParameters = scannerReadParameters.nextLine();
                                    switch (selectionReadParameters)
                                    {
                                        case "1":
                                            System.out.println("Set Stream type: ");
                                            Scanner scannerStreamType = new Scanner(System.in);
                                            try
                                            {
                                                String[] newStreamType = scannerStreamType.nextLine().trim().split(",");
                                                for (int i = 0; i < newStreamType.length; i++)
                                                {
                                                    newStreamType[i] = newStreamType[i].trim();
                                                }
                                                streamType = newStreamType;
                                            } catch (Exception ex)
                                            {
                                                //Handle the error
                                            }

                                            break;
                                        case "2":

                                            System.out.println("Set Number of streams: ");
                                            Scanner scannerNumberOfStreams = new Scanner(System.in);
                                            try
                                            {
                                                String newNumberOfStreams = scannerNumberOfStreams.nextLine().trim();
                                                nrStreams = Integer.parseInt(newNumberOfStreams);
                                            } catch (Exception ex)
                                            {
                                                //Handle the error
                                            }
                                            break;
                                        case "3":
                                            System.out.println("Set Total number of integers: ");
                                            Scanner scannerTotalNrOfIntegers = new Scanner(System.in);
                                            try
                                            {
                                                String[] newTotalNrOfIntegers = scannerTotalNrOfIntegers.nextLine().trim().split(",");
                                                totalNrOfIntegers = new int[newTotalNrOfIntegers.length];
                                                for (int i = 0; i < newTotalNrOfIntegers.length; i++)
                                                {
                                                    totalNrOfIntegers[i] = Integer.parseInt(newTotalNrOfIntegers[i].trim());
                                                }
                                            } catch (Exception ex)
                                            {
                                                log.error(ex);
                                            }
                                            break;
                                        case "4":
                                            System.out.println("Set Buffer Size: ");
                                            Scanner scannerBufferSize = new Scanner(System.in);
                                            try
                                            {
                                                String[] newBufferSize = scannerBufferSize.nextLine().trim().split(",");
                                                buffer = new HashMap<Integer, Integer>();
                                                buffer.put(Integer.parseInt(newBufferSize[0].trim()), Integer.parseInt(newBufferSize[1].trim()));
                                            } catch (Exception ex)
                                            {
                                                //Handle the error
                                            }
                                            break;
                                        case "5":
                                            System.out.println("Set Loops: ");
                                            Scanner scannerLoops = new Scanner(System.in);
                                            try
                                            {
                                                String newLoops = scannerLoops.nextLine().trim();
                                                nrLoops = Integer.parseInt(newLoops);
                                            } catch (Exception ex)
                                            {
                                                //Handle the error
                                            }
                                            break;
                                        case "E":
                                            selectionRead = "E";
                                            selectionMain = "";
                                            break;
                                        case "B":
                                            break;
                                        default:
                                            System.out.println("The selected value doesn't exist!");
                                    }
                                }
                                while (!selectionReadParameters.equals("B") && !selectionReadParameters.equals("E"));

                                break;
                            case "3":

                                // The read function is performed with values defined in the csv file.
                                System.out.println("Set parameters file full path: ");
                                Scanner readFilePath = new Scanner(System.in);
                                String filePath = readFilePath.nextLine();
                                allParameters = ReadWriteCsvFile(filePath);
                                selectionRead = "E";
                                break;
                            case "B":
                                selectionMain = "";
                                break;
                            default:
                                System.out.println("The selected value doesn't exist!");
                        }
                    } while (!selectionRead.equals("B") && !selectionRead.equals("E"));
                    // After specifying the values by one of the above mentioned ways, it is time to execute the
                    // the read function.
                    if (selectionRead.equals("E"))
                    {
                        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd_HH_mm_ss");
                        Date date = new Date();
                        String datePart = dateFormat.format(date);
                        String resultsReadFilename = "ReadResults_"+datePart+".csv";

                        java.io.FileWriter fileReaderResults = new java.io.FileWriter(resultsReadFilename);
                        //Write the CSV file header
                        fileReaderResults.append(READ_FILE_HEADER);
                        //Add a new line separator after the header
                        fileReaderResults.append(READ_NEW_LINE_SEPARATOR);


                        // The case when parameters are defined in csv files.
                        for (Parameters parameter : allParameters)
                        {
                            if (parameter != null)
                            {
                                nrStreams = parameter.getNrStreams();
                                totalNrOfIntegers = parameter.getTotalNrOfIntegers();
                                streamType = new String[]{parameter.getStreamType()};
                                buffer = parameter.getBuffer();
                                nrLoops = parameter.getNrLoops();
                            }
                                for (int totalNrOfInteger : totalNrOfIntegers)
                                {
                                    for (String type : streamType)
                                    {
                                        if (type.equals("BufferB") || type.equals("Mapping"))
                                        {
                                            Map.Entry<Integer, Integer> entry = buffer.entrySet().iterator().next();
                                            for (int i = entry.getKey(); i <= entry.getValue(); i++)
                                            {
                                                String outputFolderName = "FilesFolder/" + String.valueOf(nrStreams) + '/' + String.valueOf(totalNrOfInteger) + '/';
                                                FileReader reader = new FileReader(outputFolderName);
                                                int calculatedBuffer = (int) Math.pow((double) 2, (double) i);
                                                reader.ReadFile(type, nrStreams, totalNrOfInteger, nrLoops, calculatedBuffer, fileReaderResults);
                                            }
                                        } else
                                        {
                                            String outputFolderName = "FilesFolder/" + String.valueOf(nrStreams) + '/' + String.valueOf(totalNrOfInteger) + '/';
                                            FileReader reader = new FileReader(outputFolderName);
                                            reader.ReadFile(type, nrStreams, totalNrOfInteger, nrLoops, 1, fileReaderResults);
                                        }
                                    }
                                }
                                try
                                {
                                    fileReaderResults.flush();
                                } catch (IOException ex)
                                {
                                    log.error(ex);
                                }
                        }


                        // The case when parameters are set manually by the user or used default values.
                        if(allParameters == null || allParameters.size() == 0)
                        {
                                for (int totalNrOfInteger : totalNrOfIntegers)
                                {
                                    for (String type : streamType)
                                    {
                                        if (type.equals("BufferB") || type.equals("Mapping"))
                                        {
                                            Map.Entry<Integer, Integer> entry = buffer.entrySet().iterator().next();
                                            for (int i = entry.getKey(); i <= entry.getValue(); i++)
                                            {
                                                String outputFolderName = "FilesFolder/" + String.valueOf(nrStreams) + '/' + String.valueOf(totalNrOfInteger) + '/';
                                                FileReader reader = new FileReader(outputFolderName);
                                                int calculatedBuffer = (int) Math.pow((double) 2, (double) i);
                                                reader.ReadFile(type, nrStreams, totalNrOfInteger, nrLoops, calculatedBuffer, fileReaderResults);
                                            }
                                        } else
                                        {
                                            String outputFolderName = "FilesFolder/" + String.valueOf(nrStreams) + '/' + String.valueOf(totalNrOfInteger) + '/';
                                            FileReader reader = new FileReader(outputFolderName);
                                            reader.ReadFile(type, nrStreams, totalNrOfInteger, nrLoops, 1, fileReaderResults);
                                        }
                                    }
                                }
                            try
                            {
                                fileReaderResults.flush();
                            } catch (IOException ex)
                            {
                                log.error(ex);
                            }
                        }
                        try
                        {
                            fileReaderResults.flush();
                            fileReaderResults.close();
                        } catch (IOException ex)
                        {
                            log.error(ex);
                        }
                    }
                    break;
                case "3":
                    // Application handles here the multi-way merge-sort algorithm.
                    do
                    {
                        System.out.println("[1] Specify your values.");
                        System.out.println("[2] Import parameter values from file.");
                        System.out.println("[B] Go back at Main menu.");

                        System.out.println("Select feature value: ");
                        Scanner scannerMWayMergeSort = new Scanner(System.in);
                        selectionMWayMergeSort = scannerMWayMergeSort.nextLine();
                        switch (selectionMWayMergeSort)
                        {
                            case "1":

                                // The user values for multi-way merge-sort algorithm are specified here. After the user chooses what value to change
                                // we read it and save in the global variables. This process may be repeated several time for each of the
                                // parameters and it is ended when the user decides to execute the algorithm or goes back at the main menu.
                                do
                                {
                                    SetValueMenuMWAY();

                                    Scanner scannerMWayMergeSortParameters = new Scanner(System.in);
                                    selectionMWayMergeSortParameters = scannerMWayMergeSortParameters.nextLine();
                                    switch (selectionMWayMergeSortParameters)
                                    {
                                        case "1":
                                            System.out.println("Set Read stream type: ");
                                            Scanner scannerReadStreamType = new Scanner(System.in);
                                            try
                                            {
                                                String newReadStreamType = scannerReadStreamType.nextLine().trim();
                                                readStreamTypeMWayMergeSort = newReadStreamType;
                                            } catch (Exception ex)
                                            {
                                                //Handle the error
                                            }

                                            break;

                                        case "2":
                                            System.out.println("Set Write stream type: ");
                                            Scanner scannerWriteStreamType = new Scanner(System.in);
                                            try
                                            {
                                                String newWriteStreamType = scannerWriteStreamType.nextLine().trim();
                                                writeStreamTypeMWayMergeSort = newWriteStreamType;
                                            } catch (Exception ex)
                                            {
                                                //Handle the error
                                            }

                                            break;
                                        case "3":

                                            System.out.println("Set Maximum number of merge streams: ");
                                            Scanner scannerMaxNoOfMergeStreams = new Scanner(System.in);
                                            try
                                            {
                                                String newMaxNoOfMergeStreams = scannerMaxNoOfMergeStreams.nextLine().trim();
                                                maxNoOfMergeStreams = Integer.parseInt(newMaxNoOfMergeStreams);
                                            } catch (Exception ex)
                                            {
                                                //Handle the error
                                            }
                                            break;
                                        case "4":
                                            System.out.println("Set Maximum number of integers that fit into memory: ");
                                            Scanner scannerMaxNrOfIntegersInMemory = new Scanner(System.in);
                                            try
                                            {
                                                String[] newMaxNrOfIntegersInMemory = scannerMaxNrOfIntegersInMemory.nextLine().trim().split(",");
                                                maxNrOfIntegersInMemory = new int[newMaxNrOfIntegersInMemory.length];
                                                for (int i = 0; i < newMaxNrOfIntegersInMemory.length; i++)
                                                {
                                                    maxNrOfIntegersInMemory[i] = Integer.parseInt(newMaxNrOfIntegersInMemory[i].trim());
                                                }
                                            } catch (Exception ex)
                                            {
                                                log.error(ex);
                                            }
                                            break;
                                        case "5":
                                            System.out.println("Set Buffer Size: ");
                                            Scanner scannerBufferMWayMergeSortSize = new Scanner(System.in);
                                            try
                                            {
                                                String[] newBufferMWayMergeSortSize = scannerBufferMWayMergeSortSize.nextLine().trim().split(",");
                                                bufferMWayMergeSort = new HashMap<Integer, Integer>();
                                                bufferMWayMergeSort.put(Integer.parseInt(newBufferMWayMergeSortSize[0].trim()), Integer.parseInt(newBufferMWayMergeSortSize[1].trim()));
                                            } catch (Exception ex)
                                            {
                                                //Handle the error
                                            }
                                            break;
                                        case "6":
                                            System.out.println("Set Loops: ");
                                            Scanner scannerLoopsMWayMergeSort = new Scanner(System.in);
                                            try
                                            {
                                                String newLoopsMWayMergeSort = scannerLoopsMWayMergeSort.nextLine().trim();
                                                nrLoopsMWayMergeSort = Integer.parseInt(newLoopsMWayMergeSort);
                                            } catch (Exception ex)
                                            {
                                                //Handle the error
                                            }
                                            break;
                                        case "7":
                                            System.out.println("Set Input filename (with absolute path): ");
                                            Scanner scannerInputFile = new Scanner(System.in);
                                            try
                                            {
                                                inputFile = scannerInputFile.nextLine().trim();
                                            } catch (Exception ex)
                                            {
                                                //Handle the error
                                            }
                                            break;
                                        case "8":
                                            System.out.println("Set Output filename (with absolute path): ");
                                            Scanner scannerOutputFile = new Scanner(System.in);
                                            try
                                            {
                                                outputFile = scannerOutputFile.nextLine().trim();
                                            } catch (Exception ex)
                                            {
                                                //Handle the error
                                            }
                                            break;
                                        case "9":
                                            System.out.println("Set Sorting order of integers. If you need descending order specify value false for this parameter.");
                                            Scanner scannerASC = new Scanner(System.in);
                                            try
                                            {
                                                String newASC = scannerASC.nextLine().trim();
                                                ASC = Boolean.parseBoolean(newASC);
                                            } catch (Exception ex)
                                            {
                                                //Handle the error
                                            }
                                            break;
                                        case "E":
                                            selectionMWayMergeSort = "E";
                                            selectionMain = "";
                                            break;
                                        case "B":
                                            break;
                                        default:
                                            System.out.println("The selected value doesn't exist!");
                                    }
                                }
                                while (!selectionMWayMergeSortParameters.equals("B") && !selectionMWayMergeSortParameters.equals("E"));
                            break;

                            case "2":

                                // The multi-way merge-sort algorithm is performed with values defined in the csv file.
                                System.out.println("Set parameters file full path: ");
                                Scanner mWayMergeFilePath = new Scanner(System.in);
                                String filePath = mWayMergeFilePath.nextLine();
                                allParameters = MWAYCsvFile(filePath);
                                selectionMWayMergeSort = "E";
                                break;


                            case "B":
                                selectionMain = "";
                                break;
                            default:
                                System.out.println("The selected value doesn't exist!");
                        }
                    } while (!selectionMWayMergeSort.equals("B") && !selectionMWayMergeSort.equals("E"));

                    // After specifying the values by one of the above mentioned ways, it is time to execute the
                    // the multi-way merge-sort algorithm.
                    if (selectionMWayMergeSort.equals("E"))
                    {
                        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd_HH_mm_ss");
                        Date date = new Date();
                        String datePart = dateFormat.format(date);
                        String resultsMWayFilename = "MWayResults_"+datePart+".csv";

                        java.io.FileWriter fileMWayResults = new java.io.FileWriter(resultsMWayFilename);
                        //Write the CSV file header
                        fileMWayResults.append(MWAY_FILE_HEADER);
                        //Add a new line separator after the header
                        fileMWayResults.append(MWAY_NEW_LINE_SEPARATOR);

                        // The case when parameters are defined in csv files.
                        for (Parameters parameter : allParameters)
                        {
                            if (parameter != null)
                            {
                                maxNoOfMergeStreams = parameter.getMaxNoOfMergeStreams();
                                maxNrOfIntegersInMemory = parameter.getMaxNrOfIntegersInMemory();
                                readStreamTypeMWayMergeSort = parameter.getReadStreamTypeMWayMergeSort();
                                writeStreamTypeMWayMergeSort = parameter.getWriteStreamTypeMWayMergeSort();
                                bufferMWayMergeSort = parameter.getBufferMWayMergeSort();
                                nrLoopsMWayMergeSort = parameter.getNrLoopsMWayMergeSort();
                                inputFile = parameter.getInputFile();
                                outputFile = parameter.getOutputFile();
                                ASC = parameter.isASC();
                            }

                            for (int totalNrOfInteger : maxNrOfIntegersInMemory)
                            {
                                if (readStreamTypeMWayMergeSort.equals("BufferB") || readStreamTypeMWayMergeSort.equals("Mapping") || writeStreamTypeMWayMergeSort.equals("Mapping") || writeStreamTypeMWayMergeSort.equals("Mapping"))
                                {
                                    Map.Entry<Integer, Integer> entry = bufferMWayMergeSort.entrySet().iterator().next();
                                    for (int i = entry.getKey(); i <= entry.getValue(); i++)
                                    {
                                        int calculatedBuffer = (int) Math.pow((double) 2, (double) i);
                                        MWayMergeSort mWay = new MWayMergeSort(totalNrOfInteger, maxNoOfMergeStreams, calculatedBuffer);
                                        mWay.Sort(readStreamTypeMWayMergeSort, writeStreamTypeMWayMergeSort, inputFile,outputFile +".dat", ASC, nrLoopsMWayMergeSort, fileMWayResults);

                                    }
                                } else
                                {
                                    MWayMergeSort mWay = new MWayMergeSort(totalNrOfInteger, maxNoOfMergeStreams, 1);
                                    mWay.Sort(readStreamTypeMWayMergeSort, writeStreamTypeMWayMergeSort, inputFile,outputFile +".dat", ASC, nrLoopsMWayMergeSort, fileMWayResults);
                                }
                            }
                            try
                            {
                                fileMWayResults.flush();
                            } catch (IOException ex)
                            {
                                log.error(ex);
                            }
                        }

                        // The case when parameters are set manually by the user.
                        if(allParameters == null || allParameters.size() == 0)
                        {
                                for (int totalNrOfInteger : maxNrOfIntegersInMemory)
                                {
                                    if (readStreamTypeMWayMergeSort.equals("BufferB") || readStreamTypeMWayMergeSort.equals("Mapping") || writeStreamTypeMWayMergeSort.equals("Mapping") || writeStreamTypeMWayMergeSort.equals("Mapping"))
                                    {
                                        Map.Entry<Integer, Integer> entry = bufferMWayMergeSort.entrySet().iterator().next();
                                        for (int i = entry.getKey(); i <= entry.getValue(); i++)
                                        {
                                            int calculatedBuffer = (int) Math.pow((double) 2, (double) i);
                                            MWayMergeSort mWay = new MWayMergeSort(totalNrOfInteger, maxNoOfMergeStreams, calculatedBuffer);
                                            mWay.Sort(readStreamTypeMWayMergeSort, writeStreamTypeMWayMergeSort, inputFile,outputFile +".dat", ASC, nrLoopsMWayMergeSort, fileMWayResults);

                                        }
                                    } else
                                    {
                                        MWayMergeSort mWay = new MWayMergeSort(totalNrOfInteger, maxNoOfMergeStreams, 1);
                                        mWay.Sort(readStreamTypeMWayMergeSort, writeStreamTypeMWayMergeSort, inputFile,outputFile +".dat", ASC, nrLoopsMWayMergeSort, fileMWayResults);
                                    }
                                }
                                try
                                {
                                    fileMWayResults.flush();
                                } catch (IOException ex)
                                {
                                    log.error(ex);
                                }
                        }
                        try
                        {
                            fileMWayResults.flush();
                            fileMWayResults.close();
                        } catch (IOException ex)
                        {
                            log.error(ex);
                        }
                    }
                    break;
                case "Q":
                    System.out.println("Close application!");
                    break;
                default:
                    System.out.println("The selected value doesn't exist!");
            }
        } while (!selectionMain.equals("Q"));
    }

    // Method used to create some blank lines in the console for aesthetic purposes.
    private static void CreateBlankLines()
    {
        System.out.println();
        System.out.println();
        System.out.println();
        System.out.println();
    }

    // Method used to set default values to run the write and read functions. It is not suggested to use them because
    // it will be a huge number of combination which will lead to long execution time.
    private static void SetDefaultValues()
    {
        nrLoops = 6;
        nrStreams = 30;
        streamType = new String[]{"Simple", "Buffer", "BufferB", "Mapping"};
        buffer = new HashMap<Integer, Integer>();
        buffer.put(0, 5);
        totalNrOfIntegers = new int[]{1000, 10000, 100000, 1000000, 10000000, 100000000, 250000000};

        bufferMWayMergeSort = new HashMap<Integer, Integer>();
        bufferMWayMergeSort.put(20, 20);
        maxNrOfIntegersInMemory = new int[]{100000, 1000000};
        maxNoOfMergeStreams = 20;
        readStreamTypeMWayMergeSort = "Mapping";
        writeStreamTypeMWayMergeSort = "Mapping";
        nrLoopsMWayMergeSort = 1;
        inputFile = "";
        outputFile = "SortedFile";
        ASC = true;
    }

    // Method used to print in the console the values of each parameter needed for read and write functions.
    private static void SetValueMenu()
    {
        System.out.println("[1] Stream type. You can assign multiple values by separating them by commas.");
        System.out.println("    Default: \"Simple\", \"Buffer\", \"BufferB\", \"Mapping\"");
        System.out.print("    Actual value: ");
        for (String stream : streamType)
        {
            System.out.print(stream + ", ");
        }
        System.out.println();
        System.out.println("[2] Number of streams. If you assign N, the system will make tests for all values from 1 to N.");
        System.out.println("    Default: 30");
        System.out.println("    Actual value: " + nrStreams);

        System.out.println("[3] Total number of integers in 1 file. You can assign multiple values by separating them by commas.");
        System.out.println("    Default: 1000, 10000, 100000, 1000000, 10000000, 100000000, 250000000");
        System.out.print("    Actual value: ");
        for (Integer num : totalNrOfIntegers)
        {
            System.out.print(num + ", ");
        }
        System.out.println();
        System.out.println("[4] Buffer size. Define the exponent range of base 2. You have to define 2 values, the start and end of the range.");
        System.out.println("    Example: If you assign the value: 1,5 the system will make tests for all values from 2^1 until 2^5.");
        System.out.println("    Default: 0,32. It means 2^0 until 2^32");
        System.out.print("    Actual value: ");
        Map.Entry<Integer, Integer> actualEntry = buffer.entrySet().iterator().next();
        System.out.print(actualEntry.getKey() + ", " + actualEntry.getValue());
        System.out.println();
        System.out.println("[5] Loops. Define the number of loops to be executed in order to have an accurate time after the benchmark.");
        System.out.println("    Default: 6.");
        System.out.print("    Actual value: ");
        System.out.print(nrLoops);

        System.out.println();
        System.out.println("[E] Execute!");
        System.out.println();
        System.out.println();
        System.out.println("[B] Go back.");

        System.out.println("Select feature value: ");
    }

    // Method used to print in the console the values of each parameter needed for the multi-way merge-sort algorithm.
    private static void SetValueMenuMWAY()
    {
        System.out.println("[1] Read Stream type.");
        System.out.println("    Default: \"Mapping\"");
        System.out.print("    Actual value: ");
        System.out.print(readStreamTypeMWayMergeSort);
        System.out.println();

        System.out.println("[2] Write Stream type.");
        System.out.println("    Default: \"Mapping\"");
        System.out.print("    Actual value: ");
        System.out.print(writeStreamTypeMWayMergeSort);
        System.out.println();

        System.out.println("[3] Maximum number of merge streams.");
        System.out.println("    Default: 20");
        System.out.println("    Actual value: " + maxNoOfMergeStreams);

        System.out.println("[4] Maximum number of integers that fit into memory. You can assign multiple values by separating them by commas.");
        System.out.println("    Default: 100000, 1000000");
        System.out.print("    Actual value: ");
        for (Integer num : maxNrOfIntegersInMemory)
        {
            System.out.print(num + ", ");
        }
        System.out.println();
        System.out.println("[5] Buffer size. Define the exponent range of base 2. You have to define 2 values, the start and end of the range.");
        System.out.println("    Example: If you assign the value: 2,5 the system will make tests for all values from 2^2 until 2^5.");
        System.out.println("    Default: 20,20. It means 2^20 only");
        System.out.print("    Actual value: ");
        Map.Entry<Integer, Integer> actualEntry = bufferMWayMergeSort.entrySet().iterator().next();
        System.out.print(actualEntry.getKey() + ", " + actualEntry.getValue());
        System.out.println();
        System.out.println("[6] Loops. Define the number of loops to be executed in order to have an accurate time after the benchmark.");
        System.out.println("    Default: 6.");
        System.out.print("    Actual value: ");
        System.out.print(nrLoopsMWayMergeSort);
        System.out.println();

        System.out.println("[7] Input file path. Define the absolute path of the input file.");
        System.out.print("    Actual value: ");
        System.out.print(inputFile);
        System.out.println();

        System.out.println("[8] Output file path. Define the absolute path of the output file.");
        System.out.print("    Actual value: ");
        System.out.print(outputFile);
        System.out.println();

        System.out.println("[9] Sorting order of integers is ascending. If you need descending order specify value false for this parameter.");
        System.out.print("    Actual value: ");
        System.out.print(ASC);
        System.out.println();

        System.out.println();
        System.out.println("[E] Execute!");
        System.out.println();
        System.out.println();
        System.out.println("[B] Go back.");

        System.out.println("Select feature value: ");
    }

    // Method used to read parameter values from csv file for the read or write functions.
    public static ArrayList<Parameters> ReadWriteCsvFile(String file)
    {

        BufferedReader fileReader = null;

        ArrayList<Parameters> allParamaters = new ArrayList<>();

        try
        {


            String row = "";

            //Create the file reader
            fileReader = new BufferedReader(new java.io.FileReader(file));

            fileReader.readLine();

            //Read the file line by line starting from the second line
            while ((row = fileReader.readLine()) != null)
            {
                //Get all tokens available in line
                String[] columns = row.split(DELIMITER);
                if (columns.length > 1)
                {
                    String[] newTotalNrOfIntegers = columns[1].trim().split(",");
                    int[] tempTotalNrOfIntegers = new int[newTotalNrOfIntegers.length];
                    for (int i = 0; i < newTotalNrOfIntegers.length; i++)
                    {
                        tempTotalNrOfIntegers[i] = Integer.parseInt(newTotalNrOfIntegers[i].trim());
                    }

                    Map<Integer, Integer> tempBuffer = new HashMap<Integer, Integer>();


                    try
                    {
                        String[] newBufferSize = columns[3].split(",");
                        tempBuffer.put(Integer.parseInt(newBufferSize[0].trim()), Integer.parseInt(newBufferSize[1].trim()));
                    } catch (Exception ex)
                    {
                        //Handle the error
                    }

                    Parameters newParameters = new Parameters(columns[0], tempTotalNrOfIntegers, Integer.parseInt(columns[2]), tempBuffer, Integer.parseInt(columns[4]));
                    allParamaters.add(newParameters);
                }
            }
        }
        catch (Exception ex)
        {
            log.error(ex);
        }
        finally
        {
            try
            {
                fileReader.close();
            }
            catch (IOException e)
            {
                log.error("Error while closing the reader in the csv file.");
            }
            return allParamaters;
        }
    }

    // Method used to read parameter values from csv file for the multi-way merge-sort algorithm.
    public static ArrayList<Parameters> MWAYCsvFile(String file)
    {

        BufferedReader fileReader = null;

        ArrayList<Parameters> allParamaters = new ArrayList<>();

        try
        {

            String row = "";

            //Create the file reader
            fileReader = new BufferedReader(new java.io.FileReader(file));

            fileReader.readLine();

            //Read the file line by line starting from the second line
            while ((row = fileReader.readLine()) != null)
            {
                //Get all tokens available in line
                String[] columns = row.split(DELIMITER);
                if (columns.length > 1)
                {
                    String readStreamType = columns[0];
                    String writeStreamType = columns[1];
                    int nrOfStreams = Integer.parseInt(columns[2]);

                    String[] newMaxNrOfIntegersMemory = columns[3].trim().split(",");
                    int[] tempMaxNrOfIntegersMemory = new int[newMaxNrOfIntegersMemory.length];
                    for (int i = 0; i < newMaxNrOfIntegersMemory.length; i++)
                    {
                        tempMaxNrOfIntegersMemory[i] = Integer.parseInt(newMaxNrOfIntegersMemory[i].trim());
                    }

                    Map<Integer, Integer> tempBuffer = new HashMap<Integer, Integer>();

                    try
                    {
                        String[] newBufferSize = columns[4].split(",");
                        tempBuffer.put(Integer.parseInt(newBufferSize[0].trim()), Integer.parseInt(newBufferSize[1].trim()));
                    } catch (Exception ex)
                    {
                        //Handle the error
                    }

                    int nrLoops = Integer.parseInt(columns[5]);
                    String inputFile = columns[6];
                    String outputFile = columns[7];
                    boolean ASC = Boolean.parseBoolean(columns[8]);

                    Parameters newMWAYParameters = new Parameters(readStreamType,writeStreamType,nrOfStreams,tempMaxNrOfIntegersMemory,tempBuffer,nrLoops,inputFile,outputFile,ASC);
                    allParamaters.add(newMWAYParameters);
                }
            }
        }
        catch (Exception ex)
        {
            log.error(ex);
        }
        finally
        {
            try
            {
                fileReader.close();
            }
            catch (IOException e)
            {
                log.error("Error while closing the reader in the csv file.");
            }
            return allParamaters;
        }
    }
}
