package teensyp;

import java.io.InputStream;
import java.io.IOException;

public class ProxyInputStream extends InputStream {
    private IInputStream stream;
    private byte[] singleByte;

    public ProxyInputStream(IInputStream stream) {
        this.stream = stream;
        this.singleByte = new byte[1];
    }

    @Override
    public void close() throws IOException {
        stream.close();
    }

    @Override
    public int read() throws IOException {
        read(singleByte);
        return (int)singleByte[0];
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        return stream.read(b, off, len);
    }
}
