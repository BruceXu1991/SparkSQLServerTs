package cn.edu.bit.linc;

import org.json.JSONArray;
import org.json.JSONException;

import java.sql.ResultSet;

/**
 * Created by admin on 2015/3/17.
 */
public class ServerTest {
        public static void main(String args[]){
            String username = "fujigao";
            String password = "1234567899";
            String result =  AccountCheck.checkAccount(username,password);
            System.out.println(result);
            ConnectJDBC conJ = new ConnectJDBC();
            String sql = "select username,password from user";
            ResultSet rs =  conJ.getAndExucuteSQL(sql);
            JSONArray jsonArray;
            try {
                 jsonArray = conJ.transformToJsonArray(rs);
                System.out.println(jsonArray.toString());
            } catch (JSONException e) {
                e.printStackTrace();
            }finally{
                JDBCUtils.releaseAll();
            }
        }
}
