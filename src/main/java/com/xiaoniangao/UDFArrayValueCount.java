package com.xiaoniangao;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.List;

//  创建：2020/01/21
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

    //该函数时返回数组中某个值出现的次数，因此返回类型是Int。
    // initialize函数接受传入参数的ObjectInspector数组，返回的是evaluate函数返回类型的ObjectInspector
    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        argumentOIs = arguments ;

        if (arguments.length !=2 ) {
            throw new UDFArgumentLengthException(
                    "The operator 'array_value_count()' accepts 2 arguments. " +
                            "One is the array input. The other one is the value to be counted");
        }
        return PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.INT);
    }

    //主要函数处理逻辑
    //该函数目前主要统计files_type中图片、视频出现的次数和字幕中空字符出现的次数
    //所以argument[1]可能是int类型的1、6或者String的""
    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        Object input_list = arguments[0].get();
        //如果传入的数据是NULL值，需要返回0. 否则碰到null值，会报错
        if(input_list==null) return new Integer(0);

        ListObjectInspector listOI = (ListObjectInspector) argumentOIs[0];
        List<?> list = listOI.getList(input_list);
        Integer count = new Integer(0);
        Object target = arguments[1].get();
        //需要判定传入的target是int还是string
        //arguments[0]数组内的数字，在hive中是bigint，java里是long，而arguments[1]的对象如果是int，需要进行转换才能比较，
        //否则会报错
        PrimitiveObjectInspector targetOI = (PrimitiveObjectInspector) argumentOIs[1];

        for(int i = 0;i<list.size();i++){
            Object list_element = listOI.getListElement(input_list,i);
            if(targetOI.getPrimitiveCategory()== PrimitiveObjectInspector.PrimitiveCategory.INT){
                //Integer和Long不能直接比较，所以需要转换。
                //Integer和Long不能直接进行类型转换，需要用parseLong(string)来转换
                Long targetLong = Long.parseLong(targetOI.getPrimitiveJavaObject(target).toString());
                if(ObjectInspectorUtils.compare(list_element, listOI.getListElementObjectInspector(),
                        targetLong,listOI.getListElementObjectInspector())==0) {
                    count++;
                }
            }else{
                if(ObjectInspectorUtils.compare(list_element, listOI.getListElementObjectInspector(),
                        target,argumentOIs[1])==0) {
                    count++;
                }
            }
        }

        return count;
        //某个版本代码为了测试各个参数的数据类型，利用getTypeName()来查看。initialize也需要改int为string
//        return listOI.getListElementObjectInspector().getTypeName()+", "+argumentOIs[1].getTypeName();

    }

    @Override
    public String getDisplayString(String[] children) {
        return null;
    }
}