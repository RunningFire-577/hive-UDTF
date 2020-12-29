import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.json.JSONArray;

import java.util.ArrayList;
import java.util.List;


public class ExplodeJsonArray extends GenericUDTF {

      //约定函数输出列的名字和类型
    @Override
    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {

        List<? extends StructField> allStructFieldRefs = argOIs.getAllStructFieldRefs();
        if(allStructFieldRefs.size()!=1)
            throw new UDFArgumentException("只能接受一个参数");
        String typeName = allStructFieldRefs.get(0).getFieldObjectInspector().getTypeName();
        if (!"string".equals(typeName))      //typeName .equals?
            throw new UDFArgumentException("输入的类型不对");
        //输出列的名字
        ArrayList<String> fieldNames = new ArrayList<>();
        fieldNames.add("actions");
        //输出列的类型
        ArrayList<ObjectInspector> fieldOIs = new ArrayList<>();
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
        //返回的是什么？
    }

    @Override
    public void process(Object[] args) throws HiveException {
        String json = args[0].toString();//函数执行的时候传进来的是什么？ 我直接用args[0]进行爆破不行吗？
        JSONArray jsonArray = new JSONArray(json);
        for (int i = 0; i < jsonArray.length(); i++) {
            String action = jsonArray.getString(i);
            String[] result = new String[1];
            result[0]=action;
            forward(result);
        }   
    }

    @Override
    public void close() throws HiveException {

    }
}
