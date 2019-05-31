package com.xfqb.auto.elsauto;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import com.alibaba.fastjson.JSONArray;
import com.xfqb.auto.elsauto.util.ElasticSearchUtil;

@RunWith(SpringRunner.class)
@SpringBootTest
public class EsTest {
	
	@Autowired
	ElasticSearchUtil elasticSearchUtil;
	

	@Test
	public void testSearchEmployeeSetListPage() {
		String orgId = "A10001015";
		String userId = "10004002032";
		String keyword ="通知";
		String name = EsouthBO.class.getSimpleName().toLowerCase();
		JSONArray serem = this.elasticSearchUtil.searchEmployee(name, name, keyword, EsouthBO.class);
		/**
		 * 草稿
		 */
		List<EsouthDTO> drafts = new ArrayList<EsouthDTO>();
		
		for (Iterator<Object> iterator = serem.iterator(); iterator.hasNext();) {
			com.alibaba.fastjson.JSONObject object = (com.alibaba.fastjson.JSONObject) iterator.next();
			String id = object.getString("id");
			System.err.println(id);
			
			String value = object.getString("value");
			System.out.println("value = "+ value);
			EsouthDTO esOuth = com.alibaba.fastjson.JSONObject.parseObject(value, EsouthDTO.class);
			System.err.println(esOuth.getFileStatus());
			if(!orgId.equals(esOuth.getOrgScreen())) {
				continue;
			}
		
			// 文件类型 1:草稿 2:流转 3:归档
			switch (esOuth.getFileStatus()) {
			case "1":
				if(userId.equals(esOuth.getUserId())) {
					drafts.add(esOuth);
				}
				break;
			default:
				break;
			}
		}
		
		for (EsouthDTO esouthDTO : drafts) {
			System.err.println("es ** " + esouthDTO.getTitle());
		}
		
	}
	
}
