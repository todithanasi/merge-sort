package ExtMemMergeSort;

import org.apache.commons.lang3.Validate;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

public class WriteStreamMapping implements WriteStream
{
    private RandomAccessFile randomAccessFile;
    private FileChannel fileChannel;
    private MappedByteBuffer mappedBuffer;
    private final int bufferSize;
    private boolean streamIsOpen = false;
    private long position = 0;

    /**
     * Creates a new memory mapped output stream to write data with the specified buffer size.
     *
     * @param bufferSize The number of bytes in the buffer.
     */
    public WriteStreamMapping(int bufferSize)
    {
        Validate.validState(bufferSize > 0, "Buffer's size must be bigger than 0.");
        this.bufferSize = bufferSize;
    }

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
        randomAccessFile = new RandomAccessFile(new File(filePath), "rw");
        fileChannel = randomAccessFile.getChannel();

        if (appendToFile)
        {
            position = fileChannel.size();
        }
        else
        {
            fileChannel.truncate(0);
        }

        mappedBuffer = (MappedByteBuffer) MappedByteBuffer.allocateDirect(bufferSize);
        streamIsOpen = true;
    }

    /**
     * Method to write an integer to the stream. We use the buffer in order to prevent to have less disk I/O.
     *
     * @param element Integer to write to the stream.
     * @throws IOException
     */
    @Override
    public void WriteElement(int element) throws IOException
    {
        Validate.validState(IsOpen());

        if (!mappedBuffer.hasRemaining())
        {

            MappedByteBuffer mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, position, bufferSize);
            mappedByteBuffer.put(mappedBuffer);

            mappedBuffer = (MappedByteBuffer) MappedByteBuffer.allocateDirect(bufferSize);
            position = position + bufferSize;
        }

        mappedBuffer.putInt(element);
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
     * Method to close the stream. We take care to write the elements that are in the last buffer
     * which may not be completely full.
     *
     * @throws IOException
     */
    @Override
    public void Close() throws IOException
    {
        if (fileChannel != null)
        {
            MappedByteBuffer mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, position, mappedBuffer.position());
            mappedBuffer.compact();
            mappedByteBuffer.put(mappedBuffer);
            mappedByteBuffer.force();
            fileChannel.close();
            randomAccessFile.close();
        }
        streamIsOpen = false;
    }
}
