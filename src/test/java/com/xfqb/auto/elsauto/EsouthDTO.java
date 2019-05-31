package com.xfqb.auto.elsauto;

import java.io.Serializable;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
/**
 * 
 * @author qiang xu
 *
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode
public class EsouthDTO implements Serializable{

	/**
	 * serialVersionUID
	 */
	private static final long serialVersionUID = 1L;
	
	/**
	 * 标题
	 */
	private String title;
	
	/**
	 * 来文号
	 */
	private String inCode;
	
	/**
	 * 来文单位
	 */
	private String inOrg;
	
	/**
	 * 收文时间
	 */
	private String inTime;
	
	/**
	 * 办结时间
	 */
	private String endTime;
	
	/**
	 * 页数
	 */
	private String count;
	
	/**
	 * 紧急程度     无:0 平件:1 急件:2  特急:3
	 */
	
	private String urgency;
	
	/**
	 * 收文分类(1.党群,2行政,3业务)
	 */
	
	private String inType;
	
	/**
	 * 审批意见
	 */
	
	private String comment;
	
	/**
	 * 审批人
	 */
	private String assigneeName;
	
	/**
	 * 文件id
	 */
	private String fileId;
	
	/**
	 * 用户的id
	 */
	private String userId;
	
	/**
	 * 文件类型   1:草稿   2:流转   3:归档
	 */
	private String fileStatus;
	
	/**
	 * 公开方式(1.公开,2内部公开,3秘密,4机密,5绝密)
	 */
	private String openType;
	
	/**
	 * orgid
	 */
	private String orgScreen;

	@Override
	public String toString() {
		return "EsouthDTO [title=" + title + ", inCode=" + inCode + ", inOrg=" + inOrg + ", inTime=" + inTime
				+ ", endTime=" + endTime + ", count=" + count + ", urgency=" + urgency + ", inType=" + inType
				+ ", comment=" + comment + ", assigneeName=" + assigneeName + ", fileId=" + fileId + ", userId="
				+ userId + ", fileStatus=" + fileStatus + ", openType=" + openType + ", orgScreen=" + orgScreen + "]";
	}

	
}
