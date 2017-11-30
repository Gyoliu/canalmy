package com.example.demo;

import com.alibaba.fastjson.JSON;
import com.google.protobuf.WireFormat;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.sql.JDBCType;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/*@RunWith(SpringRunner.class)
@SpringBootTest*/
public class CanalApplicationTests {

	@Test
	public void contextLoads() {
		/*Map<String,String> map = new HashMap<>();
		map.put("version","1");
		System.out.println(JSON.toJSONString(map));*/

		/*Object a = 100;
		System.out.println(a.getClass());
		String s =  "VARCHAR";
		System.out.println(JDBCType.valueOf(s));*/

		/*String s1 = "int(11)";
		Pattern p = Pattern.compile("^[a-z]+$");
		Matcher matcher = p.matcher(s1);
		System.out.println(matcher.find());*/

		Pattern p=Pattern.compile("[a-z]+");
		Matcher m=p.matcher("int(11)");
		while(m.find()) {
			System.out.println(m.group());
		}
	}

}
