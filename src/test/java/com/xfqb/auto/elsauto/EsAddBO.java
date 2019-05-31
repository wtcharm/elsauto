package com.xfqb.auto.elsauto;
/**
 * Es搜索添加测试
 * @author LouYue
 *
 */
public class EsAddBO {

	private String id;
	
	private String testStr;
	
	private String message;

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getTestStr() {
		return testStr;
	}

	public void setTestStr(String testStr) {
		this.testStr = testStr;
	}

	public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
	}

	@Override
	public String toString() {
		return "EsAddBO [id=" + id + ", testStr=" + testStr + ", message=" + message + "]";
	}

	
	public EsAddBO() {
		super();
	}

	public EsAddBO(String id, String testStr, String message) {
		super();
		this.id = id;
		this.testStr = testStr;
		this.message = message;
	}
	
	
	
}
