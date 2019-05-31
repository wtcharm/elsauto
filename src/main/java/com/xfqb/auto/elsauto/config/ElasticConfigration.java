package com.xfqb.auto.elsauto.config;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.apache.commons.lang.StringUtils;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesResponse;
import org.elasticsearch.action.admin.indices.alias.exists.AliasesExistResponse;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesResponse;
import org.elasticsearch.action.admin.indices.close.CloseIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.exists.types.TypesExistsResponse;
import org.elasticsearch.action.admin.indices.open.OpenIndexResponse;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.MultiSearchRequestBuilder;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.MultiSearchResponse.Item;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.UnmodifiableIterator;

/**
 * 底层操作类
 * @ClassName: ElasticConfigration 
 * @date 2018年11月20日
 * @author tang wang
 *
 */

@Component
public class ElasticConfigration {

	private final Logger logger = LoggerFactory.getLogger(ElasticConfigration.class);

	@Autowired
	private Client client;

	private BulkProcessor bulkProcessor;

	@PostConstruct
	public void initBulkProcessor() {

		bulkProcessor = BulkProcessor.builder(client, new BulkProcessor.Listener() {

			@Override
			public void beforeBulk(long executionId, BulkRequest request) {
				logger.info("序号：{} 开始执行{} 条记录保存", executionId, request.numberOfActions());
			}

			@Override
			public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
				logger.error(String.format("序号：%s 执行失败; 总记录数：%s", executionId, request.numberOfActions()), failure);
			}

			@Override
			public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
				logger.info("序号：{} 执行{}条记录保存成功,耗时：{}毫秒,", executionId,
						request.numberOfActions(), response.getTookInMillis());
			}
		}).setBulkActions(1000)
				.setBulkSize(new ByteSizeValue(10, ByteSizeUnit.MB))
				.setConcurrentRequests(4)
				.setFlushInterval(TimeValue.timeValueSeconds(5))
				/**
				 * 失败后等待多久及重试次数
				 */
				.setBackoffPolicy(BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(500), 3))  
				.build();
	}

	@PreDestroy
	public void closeBulk() {
		if (bulkProcessor != null) {
			try {
				bulkProcessor.close();
			} catch (Exception e) {
				logger.error("close bulkProcessor exception", e);
			}
		}
	}

	/**
	 * 判断当前索引是否存在 
	 * @param indices 索引名称
	 * @return    存在返回 true 否则返回false
	 * @author tao wang
	 * @date 2018年12月3日
	 */

	public boolean isIndexExists(String indices) {
		IndicesExistsResponse response = adminClient().prepareExists(indices).get(); 
		logger.info("判断索引...");
		return response.isExists(); 
	}

	/**
	 * 判断当前索引的类型是否存在
	 * @param indices 	索引名称
	 * @param type 		索引类型
	 * @return   存在返回 true 否则返回false
	 * @author tao wang
	 * @date 2018年12月3日
	 */

	public boolean isTypeExists(String indices, String type) {
		TypesExistsResponse response = adminClient().prepareTypesExists(indices)
				.setTypes(type).get();
		logger.info("判断索引类型...");
		return response.isExists();
	}

	/**
	 * 进行关闭索引
	 * @param indices 索引名称
	 * @return   关闭成功返回true 否则返回false
	 * @author tao wang
	 * @date 2018年12月3日
	 */

	public boolean closeIndex(String indices) {
		CloseIndexResponse response = adminClient().prepareClose(indices).get();
		logger.info("关闭索引...");
		return response.isAcknowledged();
	}

	/**
	 * 进行打开指定的索引
	 * @param indices		索引名称
	 * @return   打开成功返回true 否则返回false
	 * @author tao wang
	 * @date 2018年12月3日
	 */

	public boolean openIndex(String indices) {
		OpenIndexResponse response = adminClient().prepareOpen(indices).get();
		logger.info("打开索引...");
		return response.isAcknowledged();
	}

	/**
	 * 为当前索引起别名
	 * @param index 索引名称
	 * @param alias 别名名称
	 * @return   创建成功返回true否则返回false
	 * @author tao wang
	 * @date 2018年12月4日
	 */
	public boolean addAliasIndex(String index, String alias) {
		IndicesAliasesResponse response = adminClient().prepareAliases().
				addAlias(index, alias).get(); 
		return response.isAcknowledged(); 
	}

	/**
	 * 判断当前别名是否存在
	 * @param aliases 别名
	 * @return   存在返回true否则返回false
	 * @author tao wang
	 * @date 2018年12月4日
	 */
	public boolean isAliasExist(String... aliases) { 
		AliasesExistResponse response = adminClient().prepareAliasesExist(aliases).get(); 
		return response.isExists(); 
	}

	/**
	 * 使用 “*” 模糊匹配查询所有的别名
	 * @param aliases 查询别名
	 * @return   返回当前别名 与别名对应的索引名称
	 *  index - 索引名称
	 *  aliases - 除了当前的别名之外还有索引拥有有的其他别名
	 * @author tao wang
	 * @date 2018年12月4日
	 */

	public JSONArray getAliasIndex(String... aliases) { 
		JSONArray result = new JSONArray();
		GetAliasesResponse response = adminClient().prepareGetAliases(aliases).get(); 
		ImmutableOpenMap<String, List<AliasMetaData>> aliasesMap = response.getAliases();
		UnmodifiableIterator<String> iterator = aliasesMap.keysIt(); 
		while (iterator.hasNext()) { 
			JSONObject obj = new JSONObject();
			String key = iterator.next(); 
			obj.put("index", key);
			StringBuffer alias = new StringBuffer();
			List<AliasMetaData> aliasMetaDataList = aliasesMap.get(key); 
			for (AliasMetaData aliasMetaData : aliasMetaDataList) {
				//别名名称
				alias.append(aliasMetaData.getAlias());
				alias.append(",");
			} 
			obj.put("aliases", alias.toString().substring(0, alias.length() - 1));
			result.add(obj);
		}
		return result;
	}

	/**
	 * 返回同一个别名下面的所有数据
	 * @param aliases 别名名称
	 * @return   返回同一别名下的所有数据
	 * @author tao wang
	 * @date 2018年12月4日
	 */
	public JSONArray getAliasArray(String aliases) {
		SearchRequestBuilder search = client.prepareSearch(aliases);
		SearchResponse res = search.get();
		return convertResponse(res);
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
		IndicesAliasesResponse response = adminClient().prepareAliases().removeAlias(index, aliases).get(); 
		return response.isAcknowledged(); 
	}

	/**
	 * 批量添加,性能最好
	 * 
	 */

	public void addDocumentToBulkProcessor(String indices, String type, Object object) {
		bulkProcessor.add(client.prepareIndex(indices, type).setSource(JSONObject.toJSONString(object)).request());
	}

	/**
	 * 添加数据 
	 * @Title:  ElasticConfigration   
	 * @Description:
	 * @param indices 索引名字 
	 * @param type    索引类型
	 * @param object  索引数据
	 * @author: tao wang 
	 * @date:   2018年11月14日
	 * @throws
	 */

	public boolean addDocument(String indices, String type, Object object) {
		IndexResponse resp = client.prepareIndex(indices, type).setSource(
				JSONObject.toJSONString(object)).get();
		logger.info("添加结果：{}", resp.toString());
		return resp.isCreated();
	}

	/**
	 * 添加指定的索引Id数据
	 * @param indices		索引名称
	 * @param type		索引类型
	 * @param id			索引Id
	 * @param object   	索引数据
	 * @author tao wang
	 * @date 2018年11月30日
	 */

	public boolean addDocument(String indices, String type, String id, Object object) {
		IndexResponse resp = client.prepareIndex(indices, type, id).setSource(
				JSONObject.toJSONString(object)).get();
		logger.info("添加结果：{}", resp.toString());
		return resp.isCreated();
	}



	/**
	 * 按照Id 进行删除
	 * @Title:  ElasticConfigration   
	 * @Description:
	 * @param index  索引名称
	 * @param type   索引类型
	 * @param id     数据Id
	 * @author: tao wang 
	 * @date:   2018年11月14日
	 * @throws
	 */

	public boolean deleteDocumentById(String index, String type, String id) {
		DeleteResponse resp = client.prepareDelete(index, type, id).get();
		logger.info("删除结果：{}", resp.toString());
		return resp.isFound();
	}

	/**
	 * 删除整个索引数据
	 * @param indices 索引名称
	 * @param type    索引类型
	 * @author tao wang
	 * @date 2018年11月30日
	 */

	public boolean deleteDocument(String indices, String type) {
		DeleteIndexResponse resp = adminClient().prepareDelete(indices).execute().actionGet();
		logger.info("删除结果：{}", resp.toString());
		return resp.isAcknowledged();
	}

	/**
	 * 按ID更新
	 * @Title:  ElasticConfigration   
	 * @Description:
	 * @param indices  索引名称
	 * @param type    索引类型
	 * @param id      数据Id
	 * @param object  数据值
	 * @author: tao wang 
	 * @date:   2018年11月14日
	 * @throws
	 */

	public boolean updateDocument(String indices, String type, String id, Object object) {
		UpdateResponse resp = client.prepareUpdate(indices, type, id).setDoc(
				JSONObject.toJSONString(object)).get();
		logger.info("更新结果：{}", resp.toString());
		return resp.isContextEmpty();
	}

	/**
	 * 按照Id修改指定的某一个字段数据
	 * @param indices   索引名称
	 * @param type		索引类型
	 * @param id		索引Id
	 * @param key		索引属性名
	 * @param value   	索引属性值
	 * @author tao wang
	 * @date 2018年11月30日
	 */

	public boolean updateDocument(String indices, String type, String id, String key, String value) {
		UpdateResponse resp = client.prepareUpdate(indices, type, id).setDoc(key, value).get();
		logger.info("更新结果：{}", resp.toString());
		return resp.isContextEmpty();
	}		

	/**
	 * 查询单个数据
	 * @param  index 引擎名称
	 * @param  type  引擎类型
	 * @param  id    引擎Id  
	 * @return   
	 * @auth tao wang
	 */

	public JSONObject getDocmentById(String index, String type, String id) {
		JSONObject obj = new JSONObject();
		GetResponse response = client.prepareGet(index, type, id).get();
		Map<String, Object> map = response.getSource();
		if (map != null) {
			obj.put("id", response.getId());
			obj.put("value", map);
			return obj;
		}
		return null;

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

	public JSONObject getDocAppointById(String index, String type, String appointId) {
		GetResponse response = client.prepareGet(index, type, appointId).get();
		return  covertNotIdClazz(response);
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
		SearchRequestBuilder builder = buildRequest(indices, type);
		BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
		/**
		 * 查询指定的数据
		 */
		boolQueryBuilder.must(QueryBuilders.matchQuery(key, value));
		builder.setQuery(boolQueryBuilder);
		SearchResponse resp = builder.get();
		SearchHits response = resp.getHits();
		JSONObject result  = new JSONObject();
		for (SearchHit searchHit : response) {
			result.put("id", searchHit.getId());
			result.put("value", searchHit.getSource());
		}
		return result;
	}


	/**
	 * 指定查询字段查询出来多条数据
	 * @param indices   索引名称
	 * @param type		索引类型
	 * @param key		索引属性名
	 * @param value     索引属性内容
	 * @return    返回多条数据
	 * @author tao wang
	 * @date 2018年11月30日
	 */

	public JSONArray getDocmentByModes(String indices, String type, String key, String value) {
		SearchRequestBuilder builder = buildRequest(indices, type);
		BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
		/**
		 * 查询指定的数据
		 */
		boolQueryBuilder.must(QueryBuilders.matchQuery(key, value));
		builder.setQuery(boolQueryBuilder);
		SearchResponse resp = builder.get();
		return convertResponse(resp);
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
		SearchRequestBuilder builder = buildRequest(indices, type);
		BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
		/**
		 * 查询指定的数据
		 */
		boolQueryBuilder.must(QueryBuilders.matchQuery(key, value));
		builder.setQuery(boolQueryBuilder);
		SearchResponse response = builder.get();
		return convertNotId(response);
	}



	/**
	 * 查询当前索引所有属性数据
	 * @param indices 索引名称
	 * @param type    索引类型
	 * @return 返回属性集合
	 * @author: tao wang 
	 * @date:   2018年11月14日
	 * @throws
	 */

	public JSONArray queryDocumentByParam(String indices, String type) {
		SearchRequestBuilder builder = buildRequest(indices, type);
		SearchResponse resp = builder.get();
		return convertResponse(resp);
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

	public JSONArray queryDocAppointByParam(String indices, String type) {
		SearchRequestBuilder builder = buildRequest(indices, type);
		SearchResponse resp = builder.get();
		return convertNotId(resp);
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

	public JSONArray queryDocAppointByParam(String indices, String type, String key, 
			String value, Integer pageNumber, Integer pageSize) {
		SearchRequestBuilder builder = buildRequest(indices, type);
		BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
		/**
		 * must 满足所有才返回
		 */
		boolQueryBuilder.must(QueryBuilders.matchQuery(key, value));
		builder.setQuery(boolQueryBuilder);
		builder.setFrom(pageNumber).setSize(pageSize);
		SearchResponse resp = builder.get();
		return convertNotId(resp);
	}


	/**
	 * 进行分页数据查询
	 * @param indices  索引名称
	 * @param type     索引类型
	 * @param key      模糊字段
	 * @param value    模糊字段
	 * @param pageNumber  当前页数
	 * @param pageSize    每页显示数据
	 * @return   
	 * @author tao wang
	 * @date 2018年11月26日
	 */

	public JSONArray queryDocumentByParam(String indices, String type, String key, 
			String value, Integer pageNumber, Integer pageSize) {
		SearchRequestBuilder builder = buildRequest(indices, type);
		BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
		/**
		 * must 满足所有才返回
		 */
		boolQueryBuilder.must(QueryBuilders.matchQuery(key,value));
		builder.setQuery(boolQueryBuilder);
		builder.setFrom(pageNumber).setSize(pageSize);
		SearchResponse resp = builder.get();
		return convertResponse(resp);
	}

	/**
	 * 进行模糊查询所有，不进行分页
	 * @param indices  索引名称
	 * @param type    索引类型
	 * @param value   模糊字段
	 * @param obj     类
	 * @return   
	 * @auth tao wang
	 * @date 2018年11月26日
	 */

	@SuppressWarnings("rawtypes")
	public JSONArray queryDocumentByParam(String indices, String type, String value, Class clazz) {

		SearchRequestBuilder builder = buildRequest(indices, type);
		BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
		// 获取实体类的所有属性信息，返回Field数组  
		Field[] fields = clazz.getDeclaredFields();  
		for (Field field : fields) {  
			/**
			 * should 满足一个都返回
			 */
			System.err.println("field ** " + field.getName());
			boolQueryBuilder.should(QueryBuilders.matchQuery(field.getName(), value));		
		}
		builder.setQuery(boolQueryBuilder);
		SearchResponse resp = builder.get();
		return convertResponse(resp);
	}


	/**
	 * 当创建时指定索引Id 并且索引Id在所传实体中唯一则调取该方法
	 * 进行模糊查询所有，不进行分页
	 * @param indices  索引名称
	 * @param type    索引类型
	 * @param value   模糊字段
	 * @param obj     类
	 * @return   
	 * @auth tao wang
	 * @date 2018年12月3日
	 */

	@SuppressWarnings("rawtypes")
	public JSONArray queryDocAppointByParam(String indices, String type, String value, Class clazz) {

		SearchRequestBuilder builder = buildRequest(indices, type);
		BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
		// 获取实体类的所有属性信息，返回Field数组  
		Field[] fields = clazz.getDeclaredFields();  
		for (Field field : fields) {  
			/**
			 * should 满足一个都返回
			 */
			boolQueryBuilder.should(QueryBuilders.matchQuery(field.getName(),value));
		}  			
		builder.setQuery(boolQueryBuilder);
		SearchResponse response = builder.get();
		return convertNotId(response);
	}


	/**
	 * 进行模糊查询以及分页
	 * @param indices  索引名称
	 * @param type    索引类型
	 * @param value   模糊字段
	 * @param pageNumber 当前页
	 * @param pageSize	每页显示数据
	 * @param obj     类
	 * @return   
	 * @auth tao wang
	 * @date 2018年11月26日
	 */

	@SuppressWarnings("rawtypes")
	public JSONArray queryDocumentByParam(String indices, String type, String value, 
			Class clazz, Integer pageNumber, Integer pageSize) {

		SearchRequestBuilder builder = buildRequest(indices, type);
		BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
		// 获取实体类的所有属性信息，返回Field数组  
		Field[] fields = clazz.getDeclaredFields();  
		for (Field field : fields) {  
			/**
			 * should 满足一个都返回
			 */
			boolQueryBuilder.should(QueryBuilders.matchQuery(field.getName(),value));
		}  			
		builder.setQuery(boolQueryBuilder);
		builder.setFrom(pageNumber).setSize(pageSize);
		SearchResponse resp = builder.get();
		return convertResponse(resp);
	}

	/**
	 * 进行模糊查询所有，不进行分页
	 * @param indices  索引名称
	 * @param type    索引类型
	 * @param value   模糊字段
	 * @param setKey     类的属性名称
	 * @return   
	 * @auth tao wang
	 * @date 2018年11月26日
	 */

	public JSONArray queryDocumentByParam(String indices, String type, String value, Set<String> setKey) {

		SearchRequestBuilder builder = buildRequest(indices, type);
		BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
		for (String string : setKey) {
			boolQueryBuilder.should(QueryBuilders.matchQuery(string, 
					value));	
		}
		builder.setQuery(boolQueryBuilder);
		SearchResponse resp = builder.get();
		return convertResponse(resp);
	}


	/**
	 * 进行模糊查询进行分页
	 * @param indices  索引名称
	 * @param type    索引类型
	 * @param value   模糊字段
	 * @param setKey     类的属性名称
	 * @param pageNumber  当前页数
	 * @param pageSize   每页显示数据
	 * @return   
	 * @auth tao wang
	 * @date 2018年11月26日
	 */

	public JSONArray queryDocumentByParam(String indices, String type, String value, 
			Set<String> setKey, Integer pageNumber, Integer pageSize) {

		SearchRequestBuilder builder = buildRequest(indices, type);
		BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
		for (String string : setKey) {
			boolQueryBuilder.should(QueryBuilders.matchQuery(string, value));	
		}
		builder.setQuery(boolQueryBuilder);
		builder.setFrom(pageNumber).setSize(pageSize);
		SearchResponse resp = builder.get();
		return convertResponse(resp);
	}


	/**
	 * 同时对多个索引的数据进行精确查询
	 * @param indicesList 查询多个索引数据
	 * @return    返回查询结果
	 * @author tao wang
	 * @date 2018年11月29日
	 */

	public JSONArray getMultiGetDous(List<Map<String, String>> indicesList) {
		JSONArray result = new JSONArray();
		//将query对象放入集合中模仿传参
		List<SearchRequestBuilder> arrList = new ArrayList<>();
		for (Map<String, String> map : indicesList) {	
			SearchRequestBuilder bq = buildRequest(map.get("indices").toLowerCase(),
					map.get("type").toLowerCase())
					.setQuery(QueryBuilders.boolQuery()
							.must(QueryBuilders.boolQuery().should(
									QueryBuilders.matchQuery(map.get("key"), map.get("value")))))
					.setExplain(true);
			arrList.add(bq);
		}
		for (SearchRequestBuilder ss : arrList) {
			MultiSearchRequestBuilder sr = client.prepareMultiSearch().add(ss);
			MultiSearchResponse mss = sr.execute().actionGet();
			Iterator<Item> it = mss.iterator();
			while (it.hasNext()) {
				MultiSearchResponse.Item item = (MultiSearchResponse.Item) it.next();
				if (item.getResponse() != null && item.getResponse().getHits() != null) {
					SearchHits hits = item.getResponse().getHits();
					for (SearchHit searchHit : hits) {
						String str = JSON.toJSONString(searchHit.getSource());
						if (StringUtils.isNotBlank(str)) {
							result.add(getReultHit(searchHit, str));
						}
					}
				}	
			}
		}
		return result;
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
		JSONObject result = new JSONObject();
		/**
		 * 查询指定父级的数据
		 */
		JSONObject father = this.getDocmentByMode(fatherIndices, fatherType, fatherKey, value);
		result.put("father", father);
		/**
		 * 查询子级数据
		 */
		JSONArray son = this.getDocmentByModes(sonIndices, sonType, sonKey, value);
		result.put("son", son);
		return result;
	}


	/**
	 * 对已经存在的索引追加字段
	 * @param indices 	索引名称
	 * @param type		索引类型
	 * @param name   	字段名称
	 * @param time		字段的默认值
	 * @author tao wang
	 * @date 2018年11月30日
	 */

	@SuppressWarnings({ "deprecation", "unused" })
	public void updateHourByScroll(String indices, String type, String name, String time) {
		SearchResponse scrollResponse = client.prepareSearch(indices).setTypes(type)
				.setSearchType(SearchType.SCAN).setSize(5000).setScroll(TimeValue.timeValueMinutes(1))
				.execute().actionGet();
		//第一次不返回数据
		long count = scrollResponse.getHits().getTotalHits();
		for (int i = 0, sum = 0; sum < count; i++) {
			scrollResponse = client.prepareSearchScroll(scrollResponse.getScrollId())
					.setScroll(TimeValue.timeValueMinutes(8))
					.execute().actionGet();
			sum += scrollResponse.getHits().hits().length;
			SearchHits searchHits = scrollResponse.getHits();
			List<UpdateRequest> list = new ArrayList<UpdateRequest>();
			for (SearchHit hit : searchHits) {
				String id = hit.getId();
				Map<String, Object> source = hit.getSource();
				//这个很重要，如果中间过程失败了，在执行时，起到过滤作用，提高效率。
				if (source.containsKey(name)) {   
					logger.error(name + "已经存在");
				} else {
					try {
						UpdateRequest uRequest;
						uRequest = new UpdateRequest()
								.index(indices)
								.type(type)
								.id(id)
								.doc(XContentFactory.jsonBuilder().startObject().
										field(name, time).endObject());
						list.add(uRequest); 
					} catch (Exception e) {
						e.printStackTrace();
					}
					//client.update(uRequest).get();  //注释上一行，就是单个提交，大数据量效率很低，用一个list来使用bulk，批量提高效率
				}
			}
			if (!list.isEmpty()) {
				// 批量执行
				BulkRequestBuilder bulkRequest = client.prepareBulk();
				for (UpdateRequest uprequest : list) {
					bulkRequest.add(uprequest);
				}
				BulkResponse bulkResponse = bulkRequest.execute().actionGet();
				if (bulkResponse.hasFailures()) {
					logger.error("批量错误！");
				}
			}
		}
	}


	/**
	 * 通用的装换返回结果
	 * @Title:  ElasticConfigration   
	 * @Description:
	 * @param response 数据
	 * @param clazz    实体类
	 * @return
	 * @author: tao wang 
	 * @date:   2018年11月14日
	 * @throws
	 */

	public JSONArray convertResponse(SearchResponse response) {
		JSONArray result = new JSONArray();
		if (response != null && response.getHits() != null) {
			for (SearchHit hit : response.getHits()) {
				Map<String, Object> source = hit.getSource();
				String str = JSONObject.toJSONString(source);
				if (StringUtils.isNotBlank(str)) {
					result.add(getReultHit(hit, str));
				}
			}
		}
		return result;
	}

	/**
	 * 当手动设置索引Id时使用
	 * @param response 数据
	 * @return    返回数据的集合
	 * @author tao wang
	 * @date 2018年12月3日
	 */

	public JSONArray convertNotId(SearchResponse response) {
		JSONArray result = new JSONArray();
		if (response != null && response.getHits() != null) {
			for (SearchHit hit : response.getHits()) {
				Map<String, Object> source = hit.getSource();
				String str = JSONObject.toJSONString(source);
				if (StringUtils.isNotBlank(str)) {
					result.add(JSONObject.parseObject(str));
				}
			}
		}
		return result;
	}


	/**
	 * 返回JSON 数据
	 * @param response 数据
	 * @return   
	 * @author tao wang
	 * @date 2018年12月3日
	 */
	public JSONObject  covertNotIdClazz(GetResponse response) {
		Map<String, Object> result = response.getSource();
		return JSONObject.parseObject(JSON.toJSONString(result));
	}

	/**
	 * 进行数据数据绑定
	 * @Title:  ElasticConfigration   
	 * @Description: 
	 * @param indices  索引名称
	 * @param type    索引类型
	 * @return
	 * @author: tao wang 
	 * @date:   2018年11月14日
	 * @throws
	 */
	public SearchRequestBuilder buildRequest(String indices, String type) {
		return client.prepareSearch(indices).setTypes(type);
	}

	/**
	 * 使用管理员用户操作es
	 * @return   返回操作
	 * @author tao wang
	 * @date 2018年12月3日
	 */

	public IndicesAdminClient adminClient() {
		IndicesAdminClient indicesAdminClient = client.admin().indices();
		return indicesAdminClient; 
	}

	/**
	 * 不使用自定义Id的时候反参数据
	 * @param searchHit 数据
	 * @param str		json
	 * @return   
	 * @author tao wang
	 * @date 2018年12月4日
	 */
	public JSONObject getReultHit(SearchHit searchHit, String str) {
		JSONObject obj = new JSONObject();
		obj.put("id", searchHit.getId());
		obj.put("value", str);
		return obj;
	}
}
