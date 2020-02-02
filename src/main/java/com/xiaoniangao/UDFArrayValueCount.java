    package com.xiaoniangao;

    import org.apache.arrow.flatbuf.Int;
    import org.apache.hadoop.hive.ql.exec.Description;
    import org.apache.hadoop.hive.ql.exec.UDF;
    import org.apache.hadoop.hive.serde2.objectinspector.*;
    import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
    import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
    import org.apache.hadoop.io.Text;


    import org.apache.hadoop.hive.ql.exec.Description;
    import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
    import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
    import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;

    import org.apache.hadoop.hive.ql.metadata.HiveException;

    import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
    import org.apache.hadoop.hive.ql.udf.generic.GenericUDFUtils;

    import java.util.ArrayList;
    import java.util.List;

    //  2020/01/21
    //  hive udf.
    //  计算array中某些值出现的次数，主要用于计算字幕中，空字幕的个数，有效字幕的个数；视频组成文件中，文件类型中图片、视频的数目。
    @Description(
            name = "array_value_count",
            value = "_FUNC_(array_column, value) - from the input array column, count the occurances of value in the array"
                    + ", and returns the count "
                    +"\nExample:\n"
                    + " > SELECT _FUNC_(array_column, value)  FROM src; \n" +
                    "    array_column:[1,1,1,1,6,6], value:1, returns 4 .\n" +
                    "    array_column:[1,1,1,1,6,6], value:6, returns 2 .\n" +
                    "    array_column: [\"\",\"my xiaoniangao\",\"\",\"\",\"\"], value: \"\", returns 4. "
    )
    public class UDFArrayValueCount extends GenericUDF {
        private GenericUDFUtils.ReturnObjectInspectorResolver returnOIResolver ;
        private ObjectInspector[] argumentOIs ;

        @Override
        public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
            argumentOIs = arguments ;
            if (arguments.length !=2 ) {
                throw new UDFArgumentLengthException(
                        "The operator 'array_value_count()' accepts 2 arguments. " +
                                "One is the array input. The other one is the value to be counted");
            }
            returnOIResolver = new GenericUDFUtils.ReturnObjectInspectorResolver(true);

            if(!(returnOIResolver.update(arguments[1]))) {
                throw new UDFArgumentTypeException(
                        2,"test: \"" + arguments[0].getTypeName() +
                        " \" and \"" + arguments[1].getTypeName() + "\"");
            }
            System.out.println(returnOIResolver.get().toString());
            //            ObjectInspector returnOi= PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.INT);
//            ObjectInspectorFactory.getStandardConstantListObjectInspector(returnOi,2);
            return PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.INT);
//            return returnOIResolver.get();
        }

        @Override
        public Object evaluate(DeferredObject[] arguments) throws HiveException {
            Object input_list = arguments[0].get();
            if(input_list==null) return new Integer(0);
            ListObjectInspector listOI = (ListObjectInspector) argumentOIs[0];
            List<?> list = listOI.getList(input_list);
            Integer count = new Integer(0);
            Object target = arguments[1].get();
            for(int i = 0;i<list.size();i++){
                
                if(list.get(i).toString().equals(target)) count++;
            }
//            ret = returnOIResolver.convertIfNecessary(count,argumentOIs[1]);
//            if(ret == null) return null;
            return count;

        }

        @Override
        public String getDisplayString(String[] children) {
            return null;
        }




        public static void main(String[] args){
            UDFArrayValueCount fun = new UDFArrayValueCount();
            ArrayList<Integer> arr = new ArrayList();
            arr.add(3);
            arr.add(6);




        }
    }