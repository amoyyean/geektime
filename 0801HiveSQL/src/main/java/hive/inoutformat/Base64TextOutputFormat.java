package hive.inoutformat;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Properties;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.util.Progressable;

/**
 * FileOutputFormat for base64 encoded text files.
 *
 * Each line is a base64-encoded record. The key is a LongWritable which is the
 * offset. The value is a BytesWritable containing the base64-decoded bytes.
 *
 * This class accepts a configurable parameter:
 * "base64.text.output.format.signature"
 *
 * The UTF-8 encoded signature will be prepended to each BytesWritable before we
 * do base64 encoding.
 */
public class Base64TextOutputFormat<K extends WritableComparable, V extends Writable>
        extends HiveIgnoreKeyTextOutputFormat<K, V> {

    /**
     * Base64RecordWriter.
     *
     */
    public static class Base64RecordWriter implements RecordWriter,
            JobConfigurable {

        RecordWriter writer;
        BytesWritable bytesWritable;

        public Base64RecordWriter(RecordWriter writer) {
            this.writer = writer;
            bytesWritable = new BytesWritable();
        }

        @Override
        public void write(Writable w) throws IOException {

            // Get input data
            byte[] input;
            int inputLength;
            if (w instanceof Text) {
                input = ((Text) w).getBytes();
                inputLength = ((Text) w).getLength();
            } else {
                assert (w instanceof BytesWritable);
                input = ((BytesWritable) w).getBytes();
                inputLength = ((BytesWritable) w).getLength();
            }

            // Add signature
            byte[] wrapped = new byte[signature.length + inputLength];
            for (int i = 0; i < signature.length; i++) {
                wrapped[i] = signature[i];
            }
            for (int i = 0; i < inputLength; i++) {
                wrapped[i + signature.length] = input[i];
            }

            // Encode
            byte[] output = base64.encode(wrapped);
            bytesWritable.set(output, 0, output.length);

            writer.write(bytesWritable);
        }

        @Override
        public void close(boolean abort) throws IOException {
            writer.close(abort);
        }

        private byte[] signature;
        private final Base64 base64 = Base64TextInputFormat.createBase64();

        @Override
        public void configure(JobConf job) {
            try {
                String signatureString = job.get("base64.text.output.format.signature");
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

    @Override
    public RecordWriter getHiveRecordWriter(JobConf jc, Path finalOutPath,
                                            Class<? extends Writable> valueClass, boolean isCompressed,
                                            Properties tableProperties, Progressable progress) throws IOException {

        Base64RecordWriter writer = new Base64RecordWriter(super
                .getHiveRecordWriter(jc, finalOutPath, BytesWritable.class,
                        isCompressed, tableProperties, progress));
        writer.configure(jc);
        return writer;
    }

}