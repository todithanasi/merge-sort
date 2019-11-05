package ExtMemMergeSort;

import java.io.BufferedInputStream;
import java.io.InputStream;

public class FindBufferSize extends BufferedInputStream
{

    public FindBufferSize(InputStream inputStream)
    {
        super(inputStream);
    }

    public int getBufferSize()
    {
        return super.buf.length;
    }
}
