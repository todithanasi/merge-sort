package ExtMemMergeSort;

import org.apache.commons.lang3.Validate;

import java.io.*;

public class WriteStreamBufferB implements WriteStream
{
    private DataOutputStream dataOutputStream;
    private boolean streamIsOpen = false;
    private int bufferBSize = 0;

    /**
     * Method to assign buffer size to the new created BufferB stream.
     *
     * @param bufferSize The number of bytes in the buffer.
     */
    public WriteStreamBufferB(int bufferSize)
    {
        Validate.validState(bufferSize > 0, "Buffer's size must be bigger than 0.");
        bufferBSize = bufferSize;
    }

    /**
     * Method to create a new file if it does not exist and opens the stream on it. We use user defined buffer size.
     *
     * @param filePath     Path of the file.
     * @param appendToFile Boolean parameter to decide if should be writing to an existing file or will overwrite the content of the file.
     * @throws IOException
     */
    @Override
    public void CreateFile(String filePath, boolean appendToFile) throws IOException
    {
        File outputFile = new File(filePath);

        outputFile.createNewFile();

        OutputStream outputStream = new FileOutputStream(outputFile, appendToFile);
        BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(outputStream, bufferBSize);
        dataOutputStream = new DataOutputStream(bufferedOutputStream);
        streamIsOpen = true;
    }

    /**
     * Method to write an integer to the stream.
     *
     * @param element Integer to write to the stream.
     * @throws IOException
     */
    @Override
    public void WriteElement(int element) throws IOException
    {
        Validate.notNull(dataOutputStream, "Stream must be open.");
        dataOutputStream.writeInt(element);
    }


    /**
     * Method to check if stream is open to be safe before manipulating the stream.
     *
     * @return True if stream is open.
     */

    @Override
    public boolean IsOpen()
    {
        return streamIsOpen;
    }

    /**
     * Method to close the stream.
     *
     * @throws IOException
     */

    @Override
    public void Close() throws IOException
    {
        if (dataOutputStream != null)
        {
            dataOutputStream.close();
        }
        streamIsOpen = false;
    }
}
