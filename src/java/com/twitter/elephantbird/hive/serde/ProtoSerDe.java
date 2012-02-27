package com.twitter.elephantbird.hive.serde;

import com.google.protobuf.Message;
import com.twitter.elephantbird.mapred.input.DeprecatedProtobufFileInputFormat;
import com.twitter.elephantbird.mapreduce.io.ProtobufWritable;
import com.twitter.elephantbird.util.Protobufs;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.io.Writable;

import java.util.Properties;

/**
 * @author Scott Fines
 *         Date: 9/21/11
 *         Time: 7:26 AM
 */
public class ProtoSerDe implements SerDe{
    private static final Log LOG = LogFactory.getLog(ProtoSerDe.class);

    private Class<? extends Message> protobufType;
    @Override
    public void initialize(Configuration configuration, Properties properties) throws SerDeException {
        String protobufClassString = properties.getProperty("protobuf.class.name");
        if(protobufClassString==null) throw new SerDeException("Cannot instantiate ProtobufHiveSerde without a property for protobuf.class.name");

        try {
            LOG.info("Initializing Protobuf hive Serde");

            Class<?> clazz = Class.forName(protobufClassString);
            //TODO -sf- get this from properties instead of just knowing?
            LOG.info("Setting protobuf class <"+protobufClassString+"> to the Config");
            Protobufs.setClassConf(configuration, DeprecatedProtobufFileInputFormat.class,clazz.asSubclass(Message.class));

            protobufType = clazz.asSubclass(Message.class);
            LOG.info("Protobuf hive serde initialized with class "+ protobufType);
        } catch (ClassNotFoundException e) {
            throw new SerDeException("ClassNotFoundException! Class looking for: "+ protobufClassString,e);
        } catch(ClassCastException cce){
            throw new SerDeException("ClassCastException! Class: "+ protobufClassString,cce);
        }
    }

    @Override
    public Class<? extends Writable> getSerializedClass() {
        return ProtobufWritable.class;
    }

    @Override
    public Writable serialize(Object o, ObjectInspector objectInspector) throws SerDeException {
        return null;  //serialization not supported
    }

    @Override
    public Object deserialize(Writable writable) throws SerDeException {
        return ((ProtobufWritable)writable).get();
    }

    @Override
    public ObjectInspector getObjectInspector() throws SerDeException {
        return ObjectInspectorFactory.getReflectionObjectInspector(protobufType, ObjectInspectorFactory.ObjectInspectorOptions.PROTOCOL_BUFFERS);
    }
}
