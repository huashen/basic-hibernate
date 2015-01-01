package org.konghao.basic.model;

import java.util.List;

/*
 * 分页对象
 */
public class Pager<T> 
{
	/*
	 * 分页大小
	 */
	private int size;
	
	/*
	 * 分页起始页	
	 */
	private int offset;
	
	/*
	 * 总记录数
	 */
	private long total;
	
	/*
	 * 分页数据
	 */
	private List<T> datas;

	public int getSize() {
		return size;
	}

	public void setSize(int size) {
		this.size = size;
	}

	public int getOffset() {
		return offset;
	}

	public void setOffset(int offset) {
		this.offset = offset;
	}

	public long getTotal() {
		return total;
	}

	public void setTotal(long total) {
		this.total = total;
	}

	public List<T> getDatas() {
		return datas;
	}

	public void setDatas(List<T> datas) {
		this.datas = datas;
	}
}
