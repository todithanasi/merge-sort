package ExtMemMergeSort;

import java.io.IOException;

public interface ReadStream
{
    public void OpenFile(String filePath) throws IOException;

    public int ReadNext();

    public boolean EndOfStream() throws IOException;

    public boolean IsOpen();

    public void Close() throws IOException;
}