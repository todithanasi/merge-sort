package ExtMemMergeSort;

import java.util.Map;

public class Parameters
{
    public String getStreamType()
    {
        return streamType;
    }

    public int[] getTotalNrOfIntegers()
    {
        return totalNrOfIntegers;
    }

    public int getNrStreams()
    {
        return nrStreams;
    }

    public Map<Integer, Integer> getBuffer()
    {
        return buffer;
    }

    public int getNrLoops()
    {
        return nrLoops;
    }

    private String streamType;
    private int[] totalNrOfIntegers;
    private int nrStreams;
    private Map<Integer, Integer> buffer ;
    private int nrLoops;

    public Map<Integer, Integer> getBufferMWayMergeSort()
    {
        return bufferMWayMergeSort;
    }

    public int[] getMaxNrOfIntegersInMemory()
    {
        return maxNrOfIntegersInMemory;
    }

    public int getMaxNoOfMergeStreams()
    {
        return maxNoOfMergeStreams;
    }

    public String getReadStreamTypeMWayMergeSort()
    {
        return readStreamTypeMWayMergeSort;
    }

    public int getNrLoopsMWayMergeSort()
    {
        return nrLoopsMWayMergeSort;
    }

    public String getWriteStreamTypeMWayMergeSort()
    {
        return writeStreamTypeMWayMergeSort;
    }

    public String getInputFile()
    {
        return inputFile;
    }

    public String getOutputFile()
    {
        return outputFile;
    }

    public boolean isASC()
    {
        return ASC;
    }

    private Map<Integer, Integer> bufferMWayMergeSort ;
    private int[] maxNrOfIntegersInMemory;
    private int maxNoOfMergeStreams;
    private String readStreamTypeMWayMergeSort;
    private String writeStreamTypeMWayMergeSort;
    private int nrLoopsMWayMergeSort;
    private String inputFile;
    private String outputFile;
    private boolean ASC;


    public Parameters(String streamType, int[] totalNrOfIntegers, int nrStreams, Map<Integer, Integer> buffer, int nrLoops )
    {
        this.streamType = streamType;
        this.totalNrOfIntegers = totalNrOfIntegers;
        this.nrStreams = nrStreams;
        this.buffer = buffer;
        this.nrLoops = nrLoops;
    }

    public Parameters(String readStreamTypeMWayMergeSort, String writeStreamTypeMWayMergeSort, int maxNoOfMergeStreams, int[]  maxNrOfIntegersInMemory, Map<Integer, Integer> bufferMWayMergeSort, int nrLoopsMWayMergeSort, String inputFile, String outputFile, boolean ASC)
    {
        this.readStreamTypeMWayMergeSort = readStreamTypeMWayMergeSort;
        this.writeStreamTypeMWayMergeSort = writeStreamTypeMWayMergeSort;
        this.maxNoOfMergeStreams = maxNoOfMergeStreams;
        this.maxNrOfIntegersInMemory = maxNrOfIntegersInMemory;
        this.bufferMWayMergeSort = bufferMWayMergeSort;
        this.nrLoopsMWayMergeSort = nrLoopsMWayMergeSort;
        this.inputFile = inputFile;
        this.outputFile = outputFile;
        this.ASC = ASC;
    }
}
