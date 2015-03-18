package cn.edu.bit.linc;

import org.apache.avro.data.Json;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.omg.CosNaming.NamingContextExtPackage.StringNameHelper;

import java.sql.*;

/**
 * this class is used to connect the jdbc and execute SQL command.
 * @author  xwc
 * @version v1
 * Created by admin on 2015/3/16.
 */
public class ConnectJDBC {

    static Connection conn=null;
    static  Statement stmt = null;
    static ResultSet rs=null;

    /**
     * the method include register driver,get connection and execute sql, then return resultset
     * @param sql
     *@return resultset the Resultset of execute sql command
     * */
      public static ResultSet getAndExucuteSQL(String sql){
        //注册获得连接getconnection

        try {
            conn = JDBCUtils.getConnection();
            stmt= conn.createStatement();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        //执行sql，获取结果集
         try {
             rs = stmt.executeQuery(sql);
        } catch (SQLException e) {
            e.printStackTrace();
            System.out.println("some problem the executeQuery");
        }
         return rs;
    }


    /**
     * this method transform the resultset into json object ,and return a jsonArray.
     * @param rs  the resultset
     * @return JSONArray
     * */
    public static JSONArray transformToJsonArray(ResultSet rs) throws JSONException {

        //建立json数组
        JSONArray array = new JSONArray();
        ResultSetMetaData metaData = null;

        //定义列数
        int columnCount = 0;
        //获取列数
        try {
            metaData = rs.getMetaData();
            columnCount = metaData.getColumnCount();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        //timejson存放时间数据
        JSONObject timeJson = new JSONObject();
        //sizeJson存放吞吐量大小
        JSONObject sizeJson = new JSONObject();
        //codejson存放返回代码
        JSONObject codeJson = new JSONObject();
        //msgJson存放返回信息
        JSONObject msgJson = new JSONObject();

        //遍历每条数据
        try {
            while (rs.next()) {
                //创建json存放resultset
                JSONObject jsonObj = new JSONObject();
                // 遍历每一列
                for (int i = 1; i <= columnCount; i++) {
                    String columnName =metaData.getColumnLabel(i);
                    //获得columnName对应的值
                    String value = rs.getString(columnName);
                    jsonObj.put(columnName, value);
                }
                array.put(jsonObj);
                //存放时间，返回代码，信息和吞吐大小
            }
          /*  timeJson.put("time","20s");
            codeJson.put("code ",100010);
            msgJson.put("msg","success");
            sizeJson.put("size","2M");
            array.put(timeJson).put(sizeJson).put(codeJson).put(msgJson);*/
        }catch(SQLException e){
            e.printStackTrace();
        }finally {
            JDBCUtils.releaseAll();
        }
        return array;
    }
}
