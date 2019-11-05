package ExtMemMergeSort;

import org.apache.commons.lang3.Validate;

import java.io.IOException;
import java.nio.IntBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public class ReadStreamMapping implements ReadStream
{
    private int bufferSize;
    private boolean streamIsOpen = false;
    private FileChannel fileChannel;
    private IntBuffer intBuffer;
    private int nextInt = 0;
    private long position = 0;
    private long fileChannelSize = 0;

    /**
     * Method to assign buffer size to the new created Mapping stream.
     * The size should be greater than 0 and multiplier of 4 because each integer is 4 bytes.
     *
     * @param bufferSize The number of bytes in the buffer.
     */

    public ReadStreamMapping(int bufferSize)
    {
        Validate.isTrue(bufferSize > 0, "Buffer's size must be bigger than 0.", bufferSize);
        Validate.isTrue(bufferSize % 4 == 0, "Buffer's size must be multiplier of 4.", bufferSize);
        this.bufferSize = bufferSize;
    }

    /**
     * Method to open a file for reading. We use user defined buffer size.
     *
     * @param filePath Path of the file.
     * @throws IOException
     */
    @Override
    public void OpenFile(String filePath) throws IOException
    {
        Path path = Paths.get(filePath);
        fileChannel = FileChannel.open(path, StandardOpenOption.READ);

        fileChannelSize = fileChannel.size();
        OpenNextBuffer();
        streamIsOpen = true;
    }

    /**
     * Method to open buffers for reading. We keep the position from the previous read in
     * order to prevent reading same part of file twice.
     *
     * @throws IOException
     */
    private void OpenNextBuffer() throws IOException
    {
        long length = fileChannel.size() - position;

        if (length > bufferSize)
        {
            length = bufferSize;
        }

        MappedByteBuffer mappedBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, position, length);
        intBuffer = mappedBuffer.asIntBuffer();
        position = position + length;
    }

    /**
     * Method to read the next integer form the stream.
     *
     * @return Next integer from the stream.
     */
    @Override
    public int ReadNext()
    {
        IsChannelOpen();
        return nextInt;
    }

    /**
     * Method to check the end of the file. We try to read the next integer.
     *
     * @return Returns true if the end of stream has been reached.
     * Returns false if an integer has been read.
     * @throws IOException
     */
    public boolean EndOfStream() throws IOException
    {
        IsChannelOpen();

        if (intBuffer.hasRemaining())
        {
            nextInt = intBuffer.get();
            return false;
        }

        if (fileChannelSize > position)
        {
            OpenNextBuffer();
            nextInt = intBuffer.get();
            return false;
        }

        return true;
    }

    private void IsChannelOpen()
    {
        Validate.isTrue(streamIsOpen, "The stream channel must be open.");
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
        IsChannelOpen();
        fileChannel.close();
        streamIsOpen = false;
    }
}

