package com.twitter.elephantbird.pig.load;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.ipc.Client;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.parser.QueryParser;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Properties;

import static org.junit.Assert.assertTrue;

/**
 * @author Scott Fines
 *         Date: 8/17/11
 *         Time: 7:18 AM
 */
public class TestProtobufLoader {
    public static ArrayList<String[]> data = new ArrayList<String[]>();
    static {
        data.add(new String[] { "1.2.3.4", "-", "-", "[01/Jan/2008:23:27:45 -0600]", "\"GET /zero.html HTTP/1.0\"", "200", "100", "\"-\"",
                "\"Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_5_4; en-us) AppleWebKit/525.18 (KHTML, like Gecko) Version/3.1.2 Safari/525.20.1\"" });
        data.add(new String[] { "1.2.3.4", "-", "-", "[01/Jan/2008:23:27:45 -0600]", "\"GET /zero.html HTTP/1.0\"", "200", "100",
                "\"http://myreferringsite.com\"",
                "\"Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_5_4; en-us) AppleWebKit/525.18 (KHTML, like Gecko) Version/3.1.2 Safari/525.20.1\"" });
        data.add(new String[] { "1.2.3.4", "-", "-", "[01/Jan/2008:23:27:45 -0600]", "\"GET /zero.html HTTP/1.0\"", "200", "100", "\"-\"",
                "\"Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_5_4; en-us) AppleWebKit/525.18 (KHTML, like Gecko) Version/3.1.2 Safari/525.20.1\"" });
    }

    public static ArrayList<DataByteArray[]> EXPECTED = new ArrayList<DataByteArray[]>();
    static {

        for (int i = 0; i < data.size(); i++) {
            ArrayList<DataByteArray> thisExpected = new ArrayList<DataByteArray>();
            for (int j = 0; j <= 2; j++) {
                thisExpected.add(new DataByteArray(data.get(i)[j]));
            }
            String temp = data.get(i)[3];
            temp = temp.replace("[", "");
            temp = temp.replace("]", "");
            thisExpected.add(new DataByteArray(temp));

            temp = data.get(i)[4];

            for (String thisOne : data.get(i)[4].split(" ")) {
                thisOne = thisOne.replace("\"", "");
                thisExpected.add(new DataByteArray(thisOne));
            }
            for (int j = 5; j <= 6; j++) {
                thisExpected.add(new DataByteArray(data.get(i)[j]));
            }
            for (int j = 7; j <= 8; j++) {
                String thisOne = data.get(i)[j];
                thisOne = thisOne.replace("\"", "");
                thisExpected.add(new DataByteArray(thisOne));
            }

            DataByteArray[] toAdd = new DataByteArray[0];
            toAdd = (thisExpected.toArray(toAdd));
            EXPECTED.add(toAdd);
        }
    }

    @Test
    public void needsRealTests() {
        assertTrue("needs real tests", true);
    }

    @Test
    public void testLoadFromPigServer() throws Exception {
        Properties props = new Properties();
//        props.put("dfs.default.name","hdfs://cloud1:9000");
        props.put("mapred.job.tracker","cloud2:9001");
        props.put("mapred.jobtracker.staging.root.dir","/scottfines");
        props.put("pig.temp.dir","/scottfines/temp");

        PigContext context= new PigContext(ExecType.LOCAL,props);
        context.setDefaultLogLevel(Level.DEBUG);

        PigServer pig = new PigServer(context);

        Logger rootLogger = Logger.getRootLogger();
        rootLogger.setLevel(Level.DEBUG);

        Logger.getLogger(Configuration.class).setLevel(Level.INFO);
        Logger.getLogger(Schema.FieldSchema.class).setLevel(Level.INFO);
        Logger.getLogger(QueryParser.class).setLevel(Level.INFO);
        Logger.getLogger(DFSClient.class).setLevel(Level.INFO);
        Logger.getLogger(Client.class).setLevel(Level.INFO);
        String filename="/scottfines/niscdev/extracts/Readings/testMR/";
        filename = filename.replace("\\", "\\\\");
        pig.registerJar("/Users/scottfines/apps/pig/pig-0.8.0-cdh3u0/lib/mdm-datastore.jar");
        pig.registerJar("/Users/scottfines/apps/pig/pig-0.8.0-cdh3u0/lib/elephant-bird-2.0.5.jar");
        pig.registerQuery("A = LOAD 'file:" + filename + "' USING com.twitter.elephantbird.pig.load.ProtobufPigLoader('coop.nisc.mdm.datastore.protobuf.IntervalReadings$Reading');");

        Iterator<?> it = pig.openIterator("A");

        int tupleCount = 0;

        while (it.hasNext()) {
            Tuple tuple = (Tuple) it.next();
            if (tuple == null)
                break;
            else {
                if(tupleCount%10000==0)
                    System.out.println(tuple);
                if(tuple.size()!=16){
                    System.out.println(tuple);
                }
                tupleCount++;
            }
        }
        System.out.println(tupleCount);
        System.out.printf("Schema: <%s>%n",pig.dumpSchema("A"));
    }


}
