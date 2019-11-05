package ExtMemMergeSort;

import java.io.IOException;

public interface WriteStream
{
    public void CreateFile(String path, boolean appendToFile) throws IOException;

    public void WriteElement(int element)throws IOException;

    public boolean IsOpen();

    public void Close() throws IOException;
}
