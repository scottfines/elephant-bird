package com.twitter.elephantbird.pig.piggybank;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.*;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import java.io.IOException;
import java.util.Map;

/**
 * @author Scott Fines
 *         Date: 12/14/11
 *         Time: 3:53 PM
 */
public class MapToValues extends EvalFunc<DataBag> {

    private final int columnNumber;

    public MapToValues(int columnNumber) {
        this.columnNumber = columnNumber;
    }

    public MapToValues(String columnNum){
        this(Integer.parseInt(columnNum));
    }

    @Override
    public DataBag exec(Tuple input) throws IOException {
        if (input == null || input.size() < 1){
            return null;
        }
        try{
            Map<String,Object> map = (Map<String,Object>)input.get(columnNumber);

            DataBag dataBag = BagFactory.getInstance().newDefaultBag();
            for(String key: map.keySet()){
                dataBag.add(TupleFactory.getInstance().newTuple(map.get(key)));
            }
            return dataBag;
        }catch(Exception e){
            getLogger().error(input,e);
            return null;
        }
    }

    @Override
    public Schema outputSchema(Schema input){
        try{
            Schema schema = new Schema();
            schema.add(input.getField(columnNumber));
            return new Schema(new Schema.FieldSchema(this.getClass().getName().toLowerCase(),schema, DataType.BAG));
        } catch (Exception e) {
            return null;
        }
    }
}
