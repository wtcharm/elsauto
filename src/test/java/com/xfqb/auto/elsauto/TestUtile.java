package com.xfqb.auto.elsauto; 
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.xfqb.auto.elsauto.util.ElasticSearchUtil;

import lombok.extern.slf4j.Slf4j;

/**
 *测试类
 * @ClassName: TestUtile 
 * @date 2018年11月21日
 * @author tang wang
 */

@RunWith(SpringRunner.class)
@SpringBootTest
@Slf4j
public class TestUtile {

	@Autowired
	ElasticSearchUtil elasticSearchUtil;

	private static final String NAME = EsAddBO.class.getSimpleName().toLowerCase();
	//注入地址服务

	/**
	 * 判断当前索引是否存在
	 *    
	 * @author tao wang
	 * @date 2018年12月3日
	 */

	@Test
	public void  testIsIndexExists() {
		String index = "user";
		boolean ui = elasticSearchUtil.isIndexExists(index);
		log.info("isIndex : " + ui);
	}

	/**
	 * 判断当前索引类型是否存在
	 * @return   
	 * @author tao wang
	 * @date 2018年12月3日
	 */

	@Test
	public void  testIsTypeExists() {
		String indices = "user";
		String type = "user";
		boolean sa = elasticSearchUtil.isTypeExists(indices, type);
		log.info("isType : " + sa);
		System.err.println("da = " + sa);
	}

	/**
	 * 进行索引的关闭
	 *    
	 * @author tao wang
	 * @date 2018年12月3日
	 */

	@Test
	public void testCloseIndex() {
		String indices = "user";
		boolean cs = elasticSearchUtil.closeIndex(indices);
		log.info("close : " + cs);
		System.err.println("da = " + cs);
	}

	/**
	 * 进行索引的开启
	 *    
	 * @author tao wang
	 * @date 2018年12月3日
	 */

	@Test
	public void testOpenIndex() {
		String indices = "user";
		boolean op = elasticSearchUtil.openIndex(indices);
		log.info("open : " + op);
		System.err.println("da = " + op);
	}



	/**
	 * 添加别名
	 *    
	 * @author tao wang
	 * @date 2018年12月4日
	 */

	@Test
	public void testAddAliasIndex() {
		String alias = "member"; // 别名
		String index = "user";   // 索引名 
		boolean add = elasticSearchUtil.addAliasIndex(index, alias);
		log.info("addAliasIndex : " + add);
		System.err.println("boolean = " + add);
	}

	/**
	 * 获取当前索引对应的别名数据
	 *    
	 * @author tao wang
	 * @date 2018年12月4日
	 */

	@Test
	public void testGetAliasIndex() {
		String alias = "member"; // 别名
		String al1 = "lieh";
		String al2 = "bbo";
		JSONArray al = elasticSearchUtil.getAliasIndex(alias, al1, al2);
		for (int i = 0; i < al.size(); i++) {
			System.err.println("str = " + al.getJSONObject(i));
		}

	}

	/**
	 * 返回同一个别名下面的所有数据
	 * @param aliases 别名名称
	 * @return   返回同一别名下的所有数据
	 * @author tao wang
	 * @date 2018年12月4日
	 */

	@Test
	public void testGetAliasArray() {
		String alias = "member"; // 别名
		JSONArray al = elasticSearchUtil.getAliasArray(alias);
		for (int i = 0; i < al.size(); i++) {
			System.err.println("str = " + al.getJSONObject(i));
		}

	}

	/**
	 * 删除别名
	 *    
	 * @author tao wang
	 * @date 2018年12月4日
	 */

	@Test
	public void testDeleteAliasIndexc() {
		String alias = "member"; // 别名	
		String al2 = "bbo";		//别名
		String index = "user";   // 索引名
		boolean add = elasticSearchUtil.deleteAliasIndex(index, al2, alias);
		System.err.println("boolean = " + add);

	}

	/**
	 * 添加索引数据
	 *    
	 * @author tao wang
	 * @date 2018年11月30日
	 */

	@Test                                                     
	public void testInsert() {
		EsAddBO bo = new EsAddBO("100011", "std", "果555f岭");
		String name = "bb";
		boolean bd = elasticSearchUtil.insert(name, NAME, JSON.parseObject(JSON.toJSONString(bo)));
		log.info("添加 = " + bd);
	}


	/**
	 * 添加索引中指定的数据
	 *    
	 * @author tao wang
	 * @date 2018年12月4日
	 */
	@Test
	public void testInsertById() {
		String id = "100012";
		EsAddBO bo = new EsAddBO(id, "国星", "测试数据");
		boolean es = elasticSearchUtil.insert(NAME, NAME, id, JSON.parseObject(JSON.toJSONString(bo)));
		System.err.println(es);
	}

	/**
	 * 添加多条数据
	 *    
	 * @author tao wang
	 * @date 2018年12月3日
	 */
	@Test
	public void testInsertAll() {
		JSONArray array = new JSONArray();
		for (int i = 0; i < 5; i++) {
			String str = UUID.randomUUID().toString().replaceAll("-", "");
			EsAddBO bo = new EsAddBO(str, "国星" + i, "测试数据" + i);
			array.add(bo);
		}
		boolean es = elasticSearchUtil.insert(NAME, NAME, array);
		System.err.println(es);
	}

	/**
	 * 删除整个索引
	 *    
	 * @author tao wang
	 * @date 2018年11月30日
	 */
	@Test
	public void testDeletIndex() {
		boolean deleteAll = elasticSearchUtil.delete(NAME, NAME);
		log.info("删除整个索引 : " + deleteAll);
	}


	/**
	 * 删除索引中指定的数据
	 *    
	 * @author tao wang
	 * @date 2018年11月30日
	 */
	@Test
	public void testDeleteById() {
		String id = "AWd369eX6ZU5XVWKT-oo";
		boolean delete = elasticSearchUtil.delete(NAME, NAME, id);
		log.info("删除单个 : " + delete);
	}

	/**
	 * 批量删除
	 *    
	 * @author tao wang
	 * @date 2018年12月3日
	 */

	@Test
	public void testDeleteAll() {
		Set<String> ids =  new HashSet<String>();
		ids.add("100012");
		ids.add("AWd34B9g6ZU5XVWKT-ok");
		boolean deleteSet = elasticSearchUtil.delete(NAME, NAME, ids);
		log.info("批量删除 : " + deleteSet);
	}

	/**
	 * 
	 * 获取单个数据
	 * @author tao wang
	 * @date 2018年12月3日
	 */

	@Test
	public void testGetDocmentById() {
		JSONObject es = elasticSearchUtil.getDocmentById(NAME, NAME, "AWdy37uP6ZU5XVWKT-oJ");
		
		System.err.println("" + String.valueOf(es) );
	}

	/**
	 * 
	 * 手动指定的索引Id存在实体字段里面则之间返回当前实体的数据
	 * @author tao wang
	 * @date 2018年12月3日
	 */

	@Test
	public void testGetDocAppointById() {
		JSONObject es = elasticSearchUtil.getDocAppointById(NAME, NAME, "AWdy37uP6ZU5XVWKT-oJ");
		System.err.println("" +  es);
	}


	/**
	 * 根据制定的属性名称查询指定的数据
	 *    
	 * @author tao wang
	 * @date 2018年12月3日
	 */
	@Test
	public void testGetDocmentByMode() {
		JSONObject es = elasticSearchUtil.getDocmentByMode(NAME, NAME, "id", "100012");
		System.err.println("" +  es);
	}


	/**
	 * 根据制定的属性名称查询指定的数据
	 *    精确查询
	 * @author tao wang
	 * @date 2018年12月3日
	 */
	@SuppressWarnings("rawtypes")
	@Test
	public void testGetDocmentByModes() {
		String name = "esouthbo";
		JSONArray es = elasticSearchUtil.getDocmentByModes(name, name, "inOrg", "吉社保函 [ 2019 ] 336692 号");
		for (Iterator iterator = es.iterator(); iterator.hasNext();) {
			JSONObject object = (JSONObject) iterator.next();
			String value = object.getString("value");
			System.err.println("value =" +  value);
			String id = object.getString("id");
			System.err.println("id =" +  id);
		}
	}

	/**
	 * 精确查询
	 * 当创建时指定索引Id 并且索引Id在所传实体中唯一则调取该方法
	 * @author tao wang
	 * @date 2018年12月3日
	 */


	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	public void testGetDocAppointByModes() {
		JSONArray es = elasticSearchUtil.getDocAppointByModes(NAME, NAME, "id", "100012");
		for (Iterator iterator = es.iterator(); iterator.hasNext();) {
			Map<String, Object> object = (Map<String, Object>) iterator.next();
			System.err.println(object);
		}
	}

	/**
	 * 根据修改指定内容
	 *    
	 * @author tao wang
	 * @date 2018年12月3日
	 */

	@Test
	public void testUpdata() {
		String id = "AWcRHwxzgF0rqfdoMmpf";
		String key = "box";
		String value = "25";
		String user = "user";
		boolean es = elasticSearchUtil.update(user, user, id, key, value);
		System.err.println(es);
	}

	/**
	 * 根据Id修改指定内容
	 *    
	 * @author tao wang
	 * @date 2018年12月3日
	 */

	@Test
	public void testUpdataById() {
		String id = "100012";
		JSONObject object = new JSONObject();
		object.put("id", id);
		object.put("testStr", "国星");
		object.put("message", "测试数据");
		boolean es = elasticSearchUtil.update(NAME, NAME, id, object);
		System.err.println(es);
	}

	/**
	 * 追加一个字段
	 *    
	 * @author tao wang
	 * @date 2018年12月3日
	 */

	@Test
	public  void testUpdateAddIndex() {
		//字段名
		String name = "orgScreen";
		String index = "esouthbo";
		boolean up = elasticSearchUtil.update(index, index, name, "A10001015");
		System.err.println("boolean = " + up);
	}


	/**
	 * 进行传统的分页模糊
	 *    
	 * @author tao wang
	 * @date 2018年12月3日
	 */

	@Test
	public void testSearchEmployeeListPage() {
		String name = "esouthbo";
		JSONArray ds = elasticSearchUtil.searchEmployee(name, name, 0, 20, "endTime", "2019-01-11 11:15:25");
		for (int i = 0; i < ds.size(); i++) {
			JSONObject json = (JSONObject) JSONObject.parse(ds.get(i).toString());
//			EsAddBO esAddBo = JSONObject.parseObject(json.getString("value"), EsAddBO.class);
			System.err.println(json);
		}
	}

	/**
	 * 
	 * 当创建时指定索引Id 并且索引Id在所传实体中唯一则调取该方法
	 * 进行分页数据查询
	 * @author tao wang
	 * @date 2018年12月3日
	 */

	@Test
	public void testSearchEmployeeAppointListPage() {
		JSONArray ds = elasticSearchUtil.searchEmployeeAppoint(NAME, NAME, 0, 20, "id", "0");
		for (int i = 0; i < ds.size(); i++) {
			EsAddBO esAddBo = JSONObject.parseObject(ds.get(i).toString(), EsAddBO.class);
			System.err.println("esAddBO" + esAddBo.toString());
		}
	}

	/**
	 * 模糊查询当前类的所有属性数据
	 * @author tao wang
	 * @date 2018年12月3日
	 */

	@SuppressWarnings("rawtypes")
	@Test
	public void testSearchEmployeeClassList() {
		JSONArray json = elasticSearchUtil.searchEmployee(NAME, NAME, "测试", EsAddBO.class);
		for (Iterator iterator = json.iterator(); iterator.hasNext();) {
			JSONObject object = (JSONObject) iterator.next();
			String id = object.getString("id");
			String value = object.getString("value");
			System.err.println("value = " + value + "id = " + id);
			EsAddBO esAddBo = JSONObject.parseObject(value, EsAddBO.class);
			System.err.println("esAddBO" + esAddBo.toString());
		}
	}

	/**
	 * 模糊查询不分页
	 *    
	 * @author tao wang
	 * @date 2018年12月3日
	 */
	
	@SuppressWarnings("rawtypes")
	@Test
	public void testSearchEmployeeAppointClassList() {
		JSONArray json = elasticSearchUtil.searchEmployeeAppoint(NAME, NAME, "测试", EsAddBO.class);
		for (Iterator iterator = json.iterator(); iterator.hasNext();) {
			Object object = (Object) iterator.next();
			EsAddBO esAddBo = JSONObject.parseObject(JSON.toJSONString(object), EsAddBO.class);
			System.err.println("esAddBO" + esAddBo.toString());
		}
	}


	/**
	 * 
	 * 模糊分页
	 * @author tao wang
	 * @date 2018年12月3日
	 */
	
	@SuppressWarnings("rawtypes")
	@Test
	public void testSearchEmployeeClassListPage() {
		JSONArray json = elasticSearchUtil.searchEmployee(NAME, NAME, "星", EsAddBO.class, 0, 20);
		for (Iterator iterator = json.iterator(); iterator.hasNext();) {
			JSONObject object = (JSONObject) iterator.next();
			String id = object.getString("id");
			String value = object.getString("value");
			System.err.println("value = " + value + "id = " + id);
			EsAddBO esAddBo = JSONObject.parseObject(value, EsAddBO.class);
			System.err.println("esAddBO" + esAddBo.toString());
		}
	}

	/**
	 * 进行指定字段的模糊
	 *    
	 * @author tao wang
	 * @date 2018年12月3日
	 */
	
	@Test
	public void testSearchEmployeeSetList() {
		Set<String> str = new HashSet<String>();
		str.add("id");
		JSONArray ds = elasticSearchUtil.searchEmployee(NAME, NAME, "0", str);
		for (int i = 0; i < ds.size(); i++) {
			JSONObject json = (JSONObject) JSONObject.parse(ds.get(i).toString());
			String value = json.getString("value");
			System.err.println("value = " + value);
			EsAddBO esAddBo = JSONObject.parseObject(value, EsAddBO.class);
			System.err.println("esAddBO" + esAddBo.toString());
		}
	}

	/**
	 * 进行指定字段的模糊分页
	 *    
	 * @author tao wang
	 * @date 2018年12月3日
	 */
	
	@Test
	public void testSearchEmployeeSetListPage() {
		Set<String> str = new HashSet<String>();
		str.add("id");
		JSONArray ds = elasticSearchUtil.searchEmployee(NAME, NAME, "0", str, 0, 10);
		for (int i = 0; i < ds.size(); i++) {
			System.err.println(ds.get(i));
			JSONObject json = (JSONObject) JSONObject.parse(ds.get(i).toString());
			String value = json.getString("value");
			System.err.println("value = " + value);
			EsAddBO esAddBo = JSONObject.parseObject(value, EsAddBO.class);
			System.err.println("esAddBO" + esAddBo.toString());
		}
	}


	/**
	 * 获取所有实体
	 *    
	 * @author tao wang
	 * @date 2018年12月3日
	 */
	@SuppressWarnings("rawtypes")
	@Test
	public void testSearchEmployeeList() {
		JSONArray json = elasticSearchUtil.searchEmployee(NAME, NAME);		
		for (Iterator iterator = json.iterator(); iterator.hasNext();) {
			JSONObject object = (JSONObject) iterator.next();
			String id = object.getString("id");
			String value = object.getString("value");
			System.err.println("value = " + value + "id = " + id);
			EsAddBO esAddBo = JSONObject.parseObject(value, EsAddBO.class);
			System.err.println("esAddBO" + esAddBo.toString());
		}
	}

	/**
	 * 获取所有实体
	 *    
	 * @author tao wang
	 * @date 2018年12月3日
	 */
	@SuppressWarnings("rawtypes")
	@Test
	public void testSearchEmployeeAppointList() {
		JSONArray json = elasticSearchUtil.searchEmployeeAppoint(NAME, NAME);		
		for (Iterator iterator = json.iterator(); iterator.hasNext();) {
			Object object = (Object) iterator.next();
			EsAddBO esAddBo = JSONObject.parseObject(JSON.toJSONString(object), EsAddBO.class);
			System.err.println("esAddBO" + esAddBo.toString());
		}
	}


	/**
	 * 进行多索引查询
	 *    
	 * @author tao wang
	 * @date 2018年12月3日
	 */
	@Test                                                     
	public void testSearchMultiGetDous() {             
		List<Map<String, String>> indicesList = new ArrayList<>();
		HashMap<String, String> map1 = new HashMap<String, String>();
		map1.put("indices", "user");
		map1.put("type", "user");
		map1.put("key", "userName");
		map1.put("value", "王涛");
		HashMap<String, String> map2 = new HashMap<String, String>();
		map2.put("indices", "esaddbo");
		map2.put("type", "esaddbo");
		map2.put("key", "id");
		map2.put("value", "100011");
		indicesList.add(map2);
		indicesList.add(map1);
		JSONArray ds = elasticSearchUtil.searchMultiGetDous(indicesList);
		for (int i = 0; i < ds.size(); i++) {
			JSONObject json = (JSONObject) JSONObject.parse(ds.get(i).toString());
			String id = json.getString("id");
			String value = json.getString("value");
			System.err.println("value = " + value);
			System.err.println("id = " + id);
		}
	}

	/**
	 * 进行父子查询
	 *    
	 * @author tao wang
	 * @date 2018年12月3日
	 */
	@Test
	public void testGetMultiGetDous() {
		String fatherIndices = "";
		String fatherType = "";
		String fatherKey = "";
		String sonIndices = "";
		String sonType = "";
		String sonKey = "";
		String value = "";
		JSONObject esl = elasticSearchUtil.getMultiGetDous(fatherIndices, fatherType, fatherKey,
				sonIndices, sonType, sonKey, value);
		String father = esl.getString("father");
		String son = esl.getString("son");
		JSONObject fobj = JSONObject.parseObject(father);
		System.err.println("fobj" + fobj);
		JSONArray sObj = JSONObject.parseArray(son);
		System.err.println("sObj" + sObj);
	}

	
	
	
	
	
	
	

} 