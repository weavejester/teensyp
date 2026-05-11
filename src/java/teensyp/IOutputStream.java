package teensyp;

import java.io.Closeable;

public interface IOutputStream extends Closeable {
    public int write(byte[] b, int off, int len);
    public void flush();
}
