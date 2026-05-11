package teensyp;

import java.io.OutputStream;
import java.io.IOException;

public class ProxyOutputStream extends OutputStream {
    private IOutputStream stream;
    private byte[] singleByte;

    public ProxyOutputStream(IOutputStream stream) {
        this.stream = stream;
        this.singleByte = new byte[1];
    }

    @Override
    public void close() throws IOException {
        stream.close();
    }

    @Override
    public void write(int b) throws IOException {
        singleByte[0] = (byte)b;
        write(singleByte);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        stream.write(b, off, len);
    }

    @Override
    public void flush() throws IOException {
        stream.flush();
    }
}
