package teensyp;

import java.io.Closeable;

public interface IInputStream extends Closeable {
    public int read(byte[] b, int off, int len);
}
