package com.twitter.elephantbird.mapred.input;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.UninitializedMessageException;
import com.twitter.elephantbird.mapreduce.io.ProtobufWritable;
import com.twitter.elephantbird.util.Protobufs;
import com.twitter.elephantbird.util.StreamSearcher;
import com.twitter.elephantbird.util.TypeRef;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.*;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 11/11/11
 *         Time: 1:36 PM
 */
public class DeprecatedProtobufFileInputFormat<M extends Message> extends FileInputFormat<LongWritable,ProtobufWritable<M>> {
    private TypeRef<M> typeRef_;


    public void setTypeRef(TypeRef<M> typeRef){
        typeRef_ = typeRef;
    }

    public static <M extends Message> Class<DeprecatedProtobufFileInputFormat> getInputFormatClass(Class<M> protoClass,JobConf conf){
        Protobufs.setClassConf(conf,DeprecatedProtobufFileInputFormat.class, protoClass);
        return DeprecatedProtobufFileInputFormat.class;
    }
    @Override
    public RecordReader<LongWritable, ProtobufWritable<M>> getRecordReader(InputSplit split, JobConf job, Reporter reporter) throws IOException {
        if(typeRef_==null){
            typeRef_ = Protobufs.getTypeRef(job,DeprecatedProtobufFileInputFormat.class);
        }
        return new DeprecatedProtobufFileReader<M>(typeRef_,job,(FileSplit)split);
    }

    private static class DeprecatedProtobufFileReader<M extends Message> implements RecordReader<LongWritable, ProtobufWritable<M>> {

        private long start_;
        private long end_;
        private long pos_;
        private FSDataInputStream fileIn_;
        private ProtobufWritable<M> protobufWritable_;
        private LongWritable keyWritable_;
        private Message.Builder protoBuilder;
        private TypeRef<M> typeRef_;

        public DeprecatedProtobufFileReader(TypeRef<M> typeRef_, JobConf job, FileSplit split) throws IOException {
            this.typeRef_=typeRef_;
            start_ = split.getStart();
            end_ = split.getLength()+start_;

            final Path file = split.getPath();

            FileSystem fileSystem = file.getFileSystem(job);
            fileIn_ = fileSystem.open(file);
            fileIn_.seek(start_);

            StreamSearcher searcher = new StreamSearcher(Protobufs.KNOWN_GOOD_POSITION_MARKER);

            if(start_!=0){
                searcher.search(fileIn_);
            }

            pos_ = fileIn_.getPos();
        }

        @Override
        public boolean next(LongWritable key, ProtobufWritable<M> value) throws IOException {
            if (pos_>end_ ||fileIn_.available()<=0){
                return false;
            }
            key.set(pos_);

            if(Protobufs.KNOWN_GOOD_POSITION_MARKER.length+pos_<=end_)
                fileIn_.skipBytes(Protobufs.KNOWN_GOOD_POSITION_MARKER.length);

            pos_ = fileIn_.getPos();

            try{
                if (protoBuilder==null){
                    protoBuilder = Protobufs.getMessageBuilder(typeRef_.getRawClass());
                }

                Message.Builder builder = protoBuilder.clone();
                final boolean success = builder.mergeDelimitedFrom(fileIn_);
                if(success){
                    value.set((M)builder.build());
                }
                return success;
            }catch (InvalidProtocolBufferException e) {
                LOG.error("Invalid Protobuf exception while building " + typeRef_.getRawClass().getName(), e);
            } catch(UninitializedMessageException ume) {
                LOG.error("Uninitialized Message Exception while building " + typeRef_.getRawClass().getName(), ume);
            }
            return false;
        }

        @Override
        public LongWritable createKey() {
            if(keyWritable_==null)
                keyWritable_ = new LongWritable();
            return keyWritable_;
        }

        @Override
        public ProtobufWritable<M> createValue() {
            if(protobufWritable_==null)
                protobufWritable_ = new ProtobufWritable<M>(typeRef_);
            return protobufWritable_;
        }

        @Override
        public long getPos() throws IOException {
            return pos_;
        }

        @Override
        public void close() throws IOException {
            if(fileIn_!=null){
                fileIn_.close();
            }
        }

        @Override
        public float getProgress() throws IOException {
            return (float)(pos_-start_)/(float)(end_-start_);
        }
    }
}
