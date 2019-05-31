package com.xfqb.auto.elsauto.util;


import java.util.List;
import java.util.Map;
import java.util.Set;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.xfqb.auto.elsauto.config.ElasticConfigration;
/**
 * 业务实现接口
 * @ClassName: ElasticSearchUtil 
 * @date 2018年11月20日
 * @author tang wang
 *
 */
@Component
public class ElasticSearchUtil {

	@Autowired
	private  ElasticConfigration elasticConfigration;
	/**
	 * 判断当前索引名称是否存在
	 * @param indices 索引名称
	 * @return    存在返回 true 否则返回false
	 * @author tao wang
	 * @date 2018年12月3日
	 */

	public boolean isIndexExists(String indices) {
		return elasticConfigration.isIndexExists(indices.toLowerCase());
	}

	/**
	 * 判断当前索引的类型是否存在
	 * @param indices 	索引名称
	 * @param type		索引类型
	 * @return   存在返回 true 否则返回false
	 * @author tao wang
	 * @date 2018年12月3日
	 */

	public boolean isTypeExists(String indices, String type) {
		return elasticConfigration.isTypeExists(indices.toLowerCase(), type.toLowerCase());
	}

	/**
	 * 进行当前索引的关闭
	 * @param indices 索引名称
	 * @return   关闭成功返回true 否则返回false
	 * @author tao wang
	 * @date 2018年12月3日
	 */

	public boolean closeIndex(String indices) {
		return elasticConfigration.closeIndex(indices.toLowerCase());
	}

	/**
	 * 进行打开指定的索引
	 * @param indices		索引名称
	 * @return   打开成功返回true 否则返回false
	 * @author tao wang
	 * @date 2018年12月3日
	 */

	public boolean openIndex(String indices) {
		return elasticConfigration.openIndex(indices.toLowerCase());
	}

	/**
	 * 给当前索引起别名
	 * @param index 索引名称
	 * @param alias 别名名称
	 * @return   成功返回true否则返回false
	 * @author tao wang
	 * @date 2018年12月4日
	 */

	public boolean addAliasIndex(String index, String alias) {
		return elasticConfigration.addAliasIndex(index.toLowerCase(), alias);
	}

	/**
	 * 判断当前别名是否存在
	 * @param aliases 别名
	 * @return   存在返回true否则返回false
	 * @author tao wang
	 * @date 2018年12月4日
	 */

	public boolean isAliasExist(String... aliases) { 
		return elasticConfigration.isAliasExist(aliases);
	}

	/**
	 * 使用 “*” 模糊匹配查询所有的别名
	 * @param aliases 查询别名
	 * @return   返回当前别名 与别名对应的索引名称
	 * index - 索引名称
	 * aliases - 除了当前的别名之外还有索引拥有有的其他别名
	 * @author tao wang
	 * @date 2018年12月4日
	 */

	public JSONArray getAliasIndex(String... aliases) {
		return elasticConfigration.getAliasIndex(aliases);
	}

	/**
	 * 返回同一个别名下面的所有数据
	 * @param aliases 别名名称
	 * @return  返回同一别名下的所有数据
	 * @author tao wang
	 * @date 2018年12月4日
	 */
	public JSONArray getAliasArray(String aliases) {
		return elasticConfigration.getAliasArray(aliases);
	}

	/**
	 * 删除当前索引的别名
	 * @param index 索引名称
	 * @param aliases 别名
	 * @return   删除成功返回true 否则返回发false
	 * @author tao wang
	 * @date 2018年12月4日
	 */

	public boolean deleteAliasIndex(String index, String... aliases) {
		return elasticConfigration.deleteAliasIndex(index.toLowerCase(), aliases);		
	}

	/**
	 * 添加指定的索引名称和类型数据
	 * @Title:  ElasticConfigUtil   
	 * @Description:
	 * @param indices 索引名称
	 * @param type    索引类型
	 * @param object  JSON 对象
	 * @return
	 * @author: tao wang 
	 * @date:   2018年11月14日
	 * @throws
	 */

	public boolean insert(String indices, String type, JSONObject object) {
		return elasticConfigration.addDocument(indices.toLowerCase(), type.toLowerCase(), object);
	}

	/**
	 * 添加指定索引Id的索引数据
	 * @param indices		索引名称
	 * @param type			索引类型
	 * @param id			索引Id			
	 * @param object		索引内容
	 * @return   
	 * @author tao wang
	 * @date 2018年11月30日
	 */

	public boolean insert(String indices, String type, String id, JSONObject object) {
		return elasticConfigration.addDocument(indices.toLowerCase(), type.toLowerCase(), id, object);
	}

	/**
	 * 添加多条数据
	 * @param indices 索引名称
	 * @param type    索引类型
	 * @param array   索引的对象集合
	 * @author tao wang
	 * @return
	 */

	public boolean insert(String indices, String type, JSONArray array) {
		for (Object object : array) {
			elasticConfigration.addDocument(indices.toLowerCase(), type.toLowerCase(), object);
		}
		return true;
	}


	/**
	 * 删除整个索引
	 * @param indices	索引名称
	 * @param type		索引属性
	 * @return   
	 * @author tao wang
	 * @date 2018年11月30日
	 */

	public boolean delete(String indices, String type) {
		return elasticConfigration.deleteDocument(indices.toLowerCase(), type.toLowerCase());
	}

	/**
	 * 删除单条数据
	 * @param  indices 索引名称
	 * @param  type   索引类型
	 * @param  id    索引Id
	 * @param  
	 * @return   
	 * @author tao wang
	 */

	public boolean delete(String indices, String type, String id) {
		return elasticConfigration.deleteDocumentById(indices.toLowerCase(), type.toLowerCase(), id);
	}

	/**
	 * 删除多条数据
	 * @param  indices 索引名称
	 * @param  type    索引类型
	 * @param  ids     id 集合  
	 * @return   
	 * @author tao wang
	 */

	public boolean delete(String indices, String type, Set<String> ids) {
		for (String id : ids) {
			elasticConfigration.deleteDocumentById(indices.toLowerCase(), type.toLowerCase(), id);
		}
		return true;
	}

	/**
	 * 获取单个数据
	 * @param  index 索引名称
	 * @param  type 索引类型
	 * @param  id   索引Id
	 * @return   查询成功返回数据 否则返回null
	 * @author tao wang
	 */
	public JSONObject getDocmentById(String indices, String type, String id) {
		return elasticConfigration.getDocmentById(indices.toLowerCase(), type.toLowerCase(), id);
	}

	/**
	 * 手动指定的索引Id存在实体字段里面则之间返回当前实体的数据
	 * @param index 索引名称
	 * @param type  索引类型
	 * @param appointId  索引Id
	 * @return   返回索引的对象数据
	 * @author tao wang
	 * @date 2018年12月3日
	 */

	public JSONObject getDocAppointById(String indices, String type, String appointId) {
		return elasticConfigration.getDocAppointById(indices.toLowerCase(), type.toLowerCase(), appointId);
	} 


	/**
	 * 根据制定的属性名称查询指定的数据
	 * @param index 索引名称
	 * @param type  索引类型
	 * @param key   属性名
	 * @param value 属性值
	 * @return  指定获取一个数据
	 * @author tao wang
	 * @date 2018年11月28日
	 */

	public JSONObject getDocmentByMode(String indices, String type, String key, String value) {
		return elasticConfigration.getDocmentByMode(indices.toLowerCase(), 
				type.toLowerCase(), key, value);
	}

	/**
	 * 指定查询字段精确查询出来多条数据
	 * @param indices   索引名称
	 * @param type		索引类型
	 * @param key		索引属性名
	 * @param value     索引属性内容
	 * @return    返回多条数据
	 * @author tao wang
	 * @date 2018年11月30日
	 */

	public JSONArray getDocmentByModes(String indices, String type, String key, String value) {
		return elasticConfigration.getDocmentByModes(indices.toLowerCase(), 
				type.toLowerCase(), key, value);
	}


	/**
	 * 精确查询
	 * 当创建时指定索引Id 并且索引Id在所传实体中唯一则调取该方法
	 * @param indices 		索引名称
	 * @param type			索引类型
	 * @param key			查询实体名称
	 * @param value			查询实体参数
	 * @return    返回实体的集合
	 * @author tao wang
	 * @date 2018年12月3日
	 */

	public JSONArray getDocAppointByModes(String indices, String type, String key, String value) {
		return elasticConfigration.getDocAppointByModes(indices.toLowerCase(), 
				type.toLowerCase(), key, value);
	}

	/**
	 * 修改单条数据
	 * @param  indices 索引名称
	 * @param  type   索引类型
	 * @param  id     索引Id
	 * @param  object  修改数据 数据不修改传原值  
	 * @return   修改成功返回true 否则返回false
	 * @author tao wang
	 */

	public boolean  update(String indices, String type, String id, JSONObject object) {
		return elasticConfigration.updateDocument(indices.toLowerCase(), type.toLowerCase(), id, object);
	}

	/**
	 * 按照Id修改指定的某一个字段数据
	 * @param indices   索引名称
	 * @param type		索引类型
	 * @param id		索引Id
	 * @param key		索引属性名
	 * @param value   	索引属性值
	 * @return 修改成功返回true 否则返回false
	 * @author tao wang
	 * @date 2018年11月30日
	 */

	public boolean update(String indices, String type, String id, String key, String value) {
		return elasticConfigration.updateDocument(indices.toLowerCase(), 
				type.toLowerCase(), id, key, value);
	}

	/**
	 * 进行追加字段
	 * @param indices  	索引名称
	 * @param type	   	索引类型
	 * @param name		字段名称
	 * @param time      字段默认值			
	 * @return   
	 * @author tao wang
	 * @date 2018年11月30日
	 */

	public boolean update(String indices, String type, String name, String time) {
		elasticConfigration.updateHourByScroll(indices.toLowerCase(), 
				type.toLowerCase(), name, time);
		return true;
	}

	/**
	 * 分页查询搜索引擎里面的数据
	 * @Title:  ElasticSearchUtil   
	 * @param pageNumber 当前页数
	 * @param pageSize  每页条数
	 * @param key    要模糊字段名称
	 * @param value  要模糊字段值
	 * @param indices 索引名称
	 * @param type    索引类型 
	 * @return
	 * @author: tao wang 
	 * @date:   2018年11月12日
	 * @throws
	 */

	public JSONArray searchEmployee(String indices, String type, Integer pageNumber, 
			Integer pageSize, String key, String value) {
		return elasticConfigration.queryDocumentByParam(indices.toLowerCase(), type.toLowerCase(),
				key, value, pageNumber, pageSize);
	}

	/**
	 * 当创建时指定索引Id 并且索引Id在所传实体中唯一则调取该方法
	 * 进行分页数据查询
	 * @param indices  索引名称
	 * @param type    索引类型
	 * @param key     模糊字段
	 * @param value   模糊字段
	 * @param pageNumber  当前页数
	 * @param pageSize    每页显示数据
	 * @return   
	 * @author tao wang
	 * @date 2018年12月3日
	 * 
	 */

	public JSONArray searchEmployeeAppoint(String indices, String type, Integer pageNumber, 
			Integer pageSize, String key, String value) {
		return elasticConfigration.queryDocAppointByParam(indices.toLowerCase(), type.toLowerCase(),
				key, value, pageNumber, pageSize);
	}


	/**
	 * 模糊查询搜索里面的数据
	 * @param indices 索引名称
	 * @param type   索引类型
	 * @param value  模糊字段名称
	 * @param clazz    类属性
	 * @return   
	 * @author tao wang
	 * @date 2018年11月26日
	 */

	@SuppressWarnings("rawtypes")
	public JSONArray searchEmployee(String indices, String type, String value, Class  clazz) {
		return elasticConfigration.queryDocumentByParam(indices.toLowerCase(), 
				type.toLowerCase(), value, clazz);
	}

	/**
	 * 当创建时指定索引Id 并且索引Id在所传实体中唯一则调取该方法
	 * 模糊查询搜索里面的数据
	 * @param indices 索引名称
	 * @param type   索引类型
	 * @param value  模糊字段名称
	 * @param clazz    类属性
	 * @return   
	 * @author tao wang
	 * @date 2018年12月3日
	 */

	@SuppressWarnings("rawtypes")
	public JSONArray searchEmployeeAppoint(String indices, String type, String value, Class  clazz) {
		return elasticConfigration.queryDocAppointByParam(indices.toLowerCase(), 
				type.toLowerCase(), value, clazz);
	}


	/**
	 * 分页模糊查询里面数据
	 * @param indices 索引名称
	 * @param type   索引类型
	 * @param value  模糊字段名称
	 * @param clazz    类属性
	 * @return   
	 * @author tao wang
	 * @date 2018年12月3日
	 */

	@SuppressWarnings("rawtypes")
	public JSONArray searchEmployee(String indices, String type, String value, Class clazz, Integer pageNumber, 
			Integer pageSize) {
		return elasticConfigration.queryDocumentByParam(indices.toLowerCase(), 
				type.toLowerCase(), value, clazz, pageNumber, pageSize);
	}

	/**
	 * 模糊查询搜索里面的数据
	 * @param indices  索引名称
	 * @param type    索引类型
	 * @param value   模糊字段名称
	 * @param setKsy  类属性名称集合   
	 * @author tao wang
	 * @date 2018年11月26日
	 */

	public JSONArray searchEmployee(String indices, String type, String value, Set<String> setKsy) {
		return elasticConfigration.queryDocumentByParam(indices.toLowerCase(), 
				type.toLowerCase(), value, setKsy);
	}

	/**
	 * 模糊分页查询搜索里面的数据
	 * @param indices  索引名称
	 * @param type    索引类型
	 * @param value   模糊字段名称
	 * @param setKsy  类属性名称集合   
	 * @author tao wang
	 * @date 2018年12月3日
	 */

	public JSONArray searchEmployee(String indices, String type, String value, 
			Set<String> setKsy, Integer pageNumber, Integer pageSize) {
		return elasticConfigration.queryDocumentByParam(indices.toLowerCase(), type.toLowerCase(),
				value, setKsy, pageNumber, pageSize);
	}

	/**
	 * 查询当前所有全部的数据
	 * @Title:  ElasticSearchUtil   
	 * @Description:
	 * @param indices 索引名称
	 * @param type    索引类型
	 * @return 
	 * @author: tao wang 
	 * @date:   2018年11月12日
	 * @throws
	 */

	public JSONArray searchEmployee(String indices, String type) {
		return elasticConfigration.queryDocumentByParam(indices.toLowerCase(), type.toLowerCase());
	}


	/**
	 * 当创建时指定索引Id 并且索引Id在所传实体中唯一则调取该方法
	 * 获取当前所有的索引实体
	 * @param indices		索引名称
	 * @param type			索引类型
	 * @return   
	 * @author tao wang
	 * @date 2018年12月3日
	 */

	public JSONArray searchEmployeeAppoint(String indices, String type) {
		return elasticConfigration.queryDocAppointByParam(indices.toLowerCase(), type.toLowerCase());

	}


	/**
	 * 进行多个索引的数据进行精确的查询
	 * @param indicesList 
	 *  参数为 list<Map> 参数为:  
	 *  indices - 索引名称   type - 索引类型
	 *  key - 属性名称   value  - 属性值
	 * @return   
	 * @author tao wang
	 * @date 2018年11月29日
	 */

	public JSONArray searchMultiGetDous(List<Map<String, String>> indicesList) {
		return elasticConfigration.getMultiGetDous(indicesList);
	}

	/**
	 * 父级与子级查询，单父级查询得到多子级
	 * @param fatherIndices 父级索引名称
	 * @param fatherType    父级索引类型
	 * @param fatherKey     父级索引属性名称
	 * @param sonIndices    子级索引名称
	 * @param sonType       子级索引类型
	 * @param sonKey        子级索引属性名称
	 * @param value			查询匹配内容
	 * @return   返回单父级与多子级数据
	 * @author tao wang
	 * @date 2018年11月30日
	 */

	public JSONObject getMultiGetDous(String fatherIndices, String fatherType, String fatherKey,
			String sonIndices, String sonType, String sonKey, String value) {
		return elasticConfigration.getMultiGetDous(fatherIndices.toLowerCase(), fatherType.toLowerCase(), 
				fatherKey, sonIndices.toLowerCase(), sonType.toLowerCase(), sonKey, value);
	}


}
