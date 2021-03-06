package hive.inoutformat;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.LineRecordReader;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;


public class Base64TextInputFormat implements InputFormat<LongWritable, BytesWritable>, JobConfigurable {

    /**
     * Base64LineRecordReader.
     *
     */
    public static class Base64LineRecordReader implements
            RecordReader<LongWritable, BytesWritable>, JobConfigurable {

        LineRecordReader reader;
        Text text;

        public Base64LineRecordReader(LineRecordReader reader) {
            this.reader = reader;
            text = reader.createValue();
        }

        @Override
        public void close() throws IOException {
            reader.close();
        }

        @Override
        public LongWritable createKey() {
            return reader.createKey();
        }

        @Override
        public BytesWritable createValue() {
            return new BytesWritable();
        }

        @Override
        public long getPos() throws IOException {
            return reader.getPos();
        }

        @Override
        public float getProgress() throws IOException {
            return reader.getProgress();
        }

        @Override
        public boolean next(LongWritable key, BytesWritable value) throws IOException {
            while (reader.next(key, text)) {
                // text -> byte[] -> value
                byte[] textBytes = text.getBytes();
                int length = text.getLength();

                // Trim additional bytes
                if (length != textBytes.length) {
                    textBytes = Arrays.copyOf(textBytes, length);
                }
                byte[] binaryData = base64.decode(textBytes);

                // compare data header with signature
                int i;
                for (i = 0; i < binaryData.length && i < signature.length
                        && binaryData[i] == signature[i]; ++i) {
                    ;
                }

                // return the row only if it's not corrupted
                if (i == signature.length) {
                    value.set(binaryData, signature.length, binaryData.length
                            - signature.length);
                    return true;
                }
            }
            // no more data
            return false;
        }

        private byte[] signature;
        private final Base64 base64 = createBase64();

        @Override
        public void configure(JobConf job) {
            try {
                String signatureString = job.get("base64.text.input.format.signature");
                if (signatureString != null) {
                    signature = signatureString.getBytes("UTF-8");
                } else {
                    signature = new byte[0];
                }
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            }
        }

    }

    TextInputFormat format;
    JobConf job;

    public Base64TextInputFormat() {
        format = new TextInputFormat();
    }

    @Override
    public void configure(JobConf job) {
        this.job = job;
        format.configure(job);
    }

    public RecordReader<LongWritable, BytesWritable> getRecordReader(
            InputSplit genericSplit, JobConf job, Reporter reporter) throws IOException {
        reporter.setStatus(genericSplit.toString());
        Base64LineRecordReader reader = new Base64LineRecordReader(
                new LineRecordReader(job, (FileSplit) genericSplit));
        reader.configure(job);
        return reader;
    }

    @Override
    public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
        return format.getSplits(job, numSplits);
    }

    /**
     * Workaround an incompatible change from commons-codec 1.3 to 1.4.
     * Since Hadoop has this jar on its classpath, we have no way of knowing
     * which version we are running against.
     */
    static Base64 createBase64() {
        try {
            // This constructor appeared in 1.4 and specifies that we do not want to
            // line-wrap or use any newline separator
            Constructor<Base64> ctor = Base64.class.getConstructor(int.class, byte[].class);
            return ctor.newInstance(0, null);
        } catch (NoSuchMethodException e) { // ie we are running 1.3
            // In 1.3, this constructor has the same behavior, but in 1.4 the default
            // was changed to add wrapping and newlines.
            return new Base64();
        } catch (InstantiationException e) {
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        } catch (InvocationTargetException e) {
            throw new RuntimeException(e.getCause());
        }
    }

}