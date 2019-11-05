package ExtMemMergeSort;

import org.apache.commons.lang3.Validate;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.File;
import java.io.FileInputStream;

public class ReadStreamSimple implements ReadStream
{
    private DataInputStream dataInputStream;
    private int nextInt = 0;
    private boolean streamIsOpen = false;

    /**
     * Method to open a file for reading.
     * @param filePath Path of the file.
     * @throws IOException
     */
    @Override
    public void OpenFile(String filePath) throws IOException
    {
        InputStream inputStream = new FileInputStream(new File(filePath));
        dataInputStream = new DataInputStream(inputStream);
        dataInputStream.readInt();
        streamIsOpen = true;
    }

    /**
     * Method to read the next integer form the stream.
     * @return Next integer from the stream.
     */
    @Override
    public int ReadNext()
    {
        Validate.isTrue(streamIsOpen, "Stream must be open.");
        return nextInt;
    }

    /**
     * Method to check the end of the file. We try to read the next integer.
     * @return Returns true if the end of stream has been reached.
     * Returns false if an integer has been read.
     * @throws IOException
     */
    @Override
    public boolean EndOfStream() throws IOException
    {
        Validate.notNull(dataInputStream, "Data input stream is not open.");

        try
        {
            nextInt = dataInputStream.readInt();
            return false;
        } catch (EOFException ex)
        {
            return true;
        }
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
        if (dataInputStream != null)
        {
            dataInputStream.close();
        }
        streamIsOpen = false;
    }
}
