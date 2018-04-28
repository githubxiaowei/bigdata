
//数据形式为三元组<S, P, O>: 
//给定一个si，给出它所有的P和O，<si, P, O> 
//给定一个oi, 给出它所有的S和P，<S, P,oi> 
//给定两个p1,p2, 给出同时拥有它们的S，<S, p1, *>, <S, p2, *> 
//给定一个oi, 给出拥有这样oi最多的S 
//试比较三种数据库下的运行效率。并尝试提高性能的可能方法，例如建立索引等。

import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Collections;
import java.util.Comparator;


import org.junit.Before;
import org.junit.After;
import org.junit.Test;

import com.csvreader.CsvReader;

public class TryRedis {
	private Jedis jedis;
	private String[] columns = {"S","P","O"};
	private String[] columnIndices = {"S_index","P_index","O_index"};
	
	@Before
	public void setup() {
		// 连接 redis 服务器
		jedis = new Jedis("localhost", 6379);
		jedis.select(1);
		System.out.println("\n检查服务是否正在运行: " + jedis.ping());
	}

	@After
	public void close() {
		jedis.close();
	}

	//@Test
	public void TestInsert() {
		jedis.flushDB();
		String filePath = "/home/xiaowei/Downloads/yagoThreeSimplified.txt";
		try {
			// 创建CSV读对象
			CsvReader csvReader = new CsvReader(filePath);
			csvReader.setDelimiter(' ');
			Integer rawID = 0;
			Integer columnNum = 3;
			while (csvReader.readRecord() && rawID < 100000) {
				++rawID;
				if (csvReader.getColumnCount() == columnNum) {
					String key_name = "ID_" + rawID.toString();
					jedis.hset("S", key_name, csvReader.get(0));
					jedis.hset("P", key_name, csvReader.get(1));
					jedis.hset("O", key_name, csvReader.get(2));
					jedis.zadd("S_index",csvReader.get(0).hashCode(),key_name);
					jedis.zadd("P_index",csvReader.get(1).hashCode(),key_name);
					jedis.zadd("O_index",csvReader.get(2).hashCode(),key_name);
				}
			}
			csvReader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		System.out.println("DBsize: " + jedis.dbSize() + "\n");
	}

	@Test
	public void TestSelectS_Slowly() {
		ArrayList<List<String>> result = selectSlow(0, "Alfredo_Covelli");
		for (List<String> iterator : result) {
			System.out.println(iterator);
		}
	}
	
	@Test
	public void TestSelectS() {
		ArrayList<List<String>> result = selectFast(0,"Peter_Whittle_politician");
		for (List<String> iterator : result) {
			System.out.println(iterator);
		}
	}

	@Test
	public void TestSelectO() {

		ArrayList<List<String>> result = selectFast(2,"UK_Independence_Party");
		for (List<String> iterator : result) {
			System.out.println(iterator);
		}
	}

	//@Test
	public void TestSelectPP() {

		String p1 = "isLeaderOf";
		String p2 = "isCitizenOf";
		ArrayList<List<String>> result1 = selectFast(1,p1);
		ArrayList<List<String>> result2 = selectFast(1,p2);
		
		HashSet<String> commonO = new HashSet<String>();
		for (List<String> iterator1 : result1) {
			for (List<String> iterator2 : result2) {
				if (iterator1.get(0).equals(iterator2.get(0)))
					commonO.add(iterator1.get(0));
			}
		}
		for (String o:commonO) {
			System.out.println(o);
		}
	}
	
	@Test
	public void TestHasMostO() {

		String o = "UK_Independence_Party";
		ArrayList<List<String>> result = selectFast(2,o);
		
		HashMap<String, Integer> occur_time = new HashMap<String,Integer>();
		for (List<String> item : result) {
			String s = item.get(0);
			if(occur_time.containsKey(s)) {
				occur_time.put(s, (Integer)(occur_time.get(s))+1);
			}else {
				occur_time.put(s, 1);
			}
		}
		List<Map.Entry<String, Integer>> list = new ArrayList<Map.Entry<String, Integer>>(occur_time.entrySet());
		Collections.sort(list,new Comparator<Map.Entry<String, Integer>>() {
			@Override
			public int compare(Map.Entry<String, Integer> kv1,
							   Map.Entry<String, Integer> kv2) {
				return kv2.getValue().compareTo(kv1.getValue());
			}
		});
		int maxTime = list.get(0).getValue();
		for (Map.Entry<String, Integer> keypair:list) {
			if(keypair.getValue()==maxTime) {
				System.out.println(keypair.getKey());
			}else {
				break;
			}
		}
	}

	/*
	 * scan all keys
	 * very very slow
	 */
	private ArrayList<List<String>> selectSlow(int field, String P) {
		ArrayList<List<String>> result = new ArrayList<List<String>>();
		
		Iterator<String> iter = jedis.hkeys(columns[field]).iterator();
		List<String> item = null;

		while (iter.hasNext()) {
			String key = iter.next();
			if (jedis.hget(columns[field], key).equals(P)) {
				item = new ArrayList<String>();
				for(String col:columns) {
					item.add(jedis.hget(col, key));
				}
				result.add(item);
			}
		}
		return result;
	}
	private ArrayList<List<String>> selectFast(int field,String value) {
		ArrayList<List<String>> result = new ArrayList<List<String>>();
		Set<String> set = jedis.zrangeByScore(columnIndices[field], value.hashCode(),value.hashCode());
		Iterator<String> iter = set.iterator();
		List<String> item = null;
		while (iter.hasNext()) {
			String key = iter.next();
			if (jedis.hget(columns[field], key).equals(value)) {
				item = new ArrayList<String>();
				for(String col:columns) {
					item.add(jedis.hget(col, key));
				}
				result.add(item);
			}
		}
		return result;
	}

	
}
