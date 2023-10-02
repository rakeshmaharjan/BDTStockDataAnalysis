package StockSparkSQL;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import scala.Tuple2;

public class Stock  implements Serializable{
	private static final long serialVersionUID = 154265L;
	public String key="";
	public List<Tuple2<String,String>> keyword;
	
	private String _tmp_stockIndex="";
	private String _tmp_keywords="";
	
	public Stock()
	{
		this.keyword=new ArrayList<Tuple2<String,String>>(); 
	}
	
	public String GetStockIndex()
	{
		if(this.keyword.isEmpty())
			return "";
		
		return this.keyword.stream().map(t -> t._1()).collect(Collectors.joining(","));
	}
	
	public String GetFoundKeywords()
	{
		if(this.keyword.isEmpty())
			return "";
		
		return this.keyword.stream().map(t -> t._2()).collect(Collectors.joining(","));
	}
	
	@Override
	public String toString()
	{
		return "|"+this.GetStockIndex()+"|"+this.GetFoundKeywords();
	}
	
	public void PutStatements(String str)
	{
		this._tmp_stockIndex=str;
		
		if(!this._tmp_stockIndex.isEmpty() && !this._tmp_keywords.isEmpty())
			this.makeTupleList();
	}
	
	public void PutKeywords(String str)
	{
		this._tmp_keywords=str;
		
		if(!this._tmp_stockIndex.isEmpty() && !this._tmp_keywords.isEmpty())
			this.makeTupleList();
	}
	
	private void makeTupleList()
	{
		String [] ar1=this._tmp_stockIndex.split(",");
		String [] ar2=this._tmp_keywords.split(",");
		
		for(int i=0;i < Math.max(ar1.length, ar2.length);i++)
		{
			String s="";
			String k="";
			
			if(i<ar1.length)
				s=ar1[i];
			
			if(i<ar2.length)
				k=ar2[i];
			
			this.keyword.add(new Tuple2<String,String>(s,k));
		}
	}
}
