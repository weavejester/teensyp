package teensyp;

import java.io.Closeable;
import java.io.Flushable;

public interface IOutputStream extends Closeable, Flushable {
    public int write(byte[] b, int off, int len);
}
