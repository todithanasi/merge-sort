package ExtMemMergeSort;

import org.apache.commons.lang3.Validate;

import java.io.*;

public class WriteStreamSimple implements WriteStream
{
    private DataOutputStream dataOutputStream;
    private boolean streamIsOpen = false;

    /**
     * Method to create a new file if it does not exist and opens the stream on it.
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
        dataOutputStream = new DataOutputStream(outputStream);
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
