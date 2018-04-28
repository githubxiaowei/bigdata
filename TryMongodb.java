
//数据形式为三元组<S, P, O>: 
//给定一个si，给出它所有的P和O，<si, P, O> 
//给定一个oi, 给出它所有的S和P，<S, P,oi> 
//给定两个p1,p2, 给出同时拥有它们的S，<S, p1, *>, <S, p2, *> 
//给定一个oi, 给出拥有这样oi最多的S 
//试比较三种数据库下的运行效率。并尝试提高性能的可能方法，例如建立索引等。

//条件查询与SQL类似。形式为
//
//等于：      {<key>:<value>}  
//小于：      {<key>:{$lt:<value>}}
//小于等于：  {<key>:{$lte:<value>}}
//大于：      {<key>:{$gt:<value>}}
//大于等于：  {<key>:{$gte:<value>}}
//不等于：    {<key>:{$ne:<value>}}
//多条件查询命令：
//
//AND:    db.student.find({key1:value1, key2:value2}).pretty()
//OR:     db.student.find({$or: [{key1: value1}, {key2:value2}]}).pretty()
//同时：  db.student.find({key1: value1, $or: [{key2:value2},{key3:value3}]}).pretty()

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.bson.Document;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;

import com.csvreader.CsvReader;


public class TryMongodb {
	private MongoClient mongoClient;
	private MongoDatabase mongoDatabase;
	private MongoCollection<Document> collection;
	private String[] columns = { "S", "P", "O" };

	@Before
	public void setup() {
		// 连接到 mongodb 服务
		mongoClient = new MongoClient("localhost", 27017);
		// 连接到数据库
		mongoDatabase = mongoClient.getDatabase("test");
		// 创建集合
		collection = mongoDatabase.getCollection("SPO");

	}

	@After
	public void close() {
		mongoClient.close();
	}

	@Test
	public void TestInsert() {
		if (collection != null) {
			collection.drop();
		}
		mongoDatabase.createCollection("SPO");
		// 选择集合
		collection = mongoDatabase.getCollection("SPO");

		String filePath = "/home/xiaowei/Downloads/yagoThreeSimplified.txt";
		try {
			List<Document> documents = new ArrayList<Document>();

			// 创建CSV读对象
			CsvReader csvReader = new CsvReader(filePath);
			csvReader.setDelimiter(' ');
			Integer rawID = 0;
			Integer columnNum = 3;
			while (csvReader.readRecord() && rawID < 1000000) {
				if (csvReader.getColumnCount() == columnNum) {
					++rawID;
					Document document = new Document();
					for (int i = 0; i < columnNum; ++i) {
						document.append(columns[i], csvReader.get(i));
					}
					documents.add(document);
				}
			}
			csvReader.close();
			// 插入文档集合
			collection.insertMany(documents);

		} catch (Exception e) {
			System.err.println(e.getClass().getName() + ": " + e.getMessage());
		}
	}

	@Test
	public void TestSelectS() {
		ArrayList<List<String>> result = select(0, "Alfredo_Covelli");
		for (List<String> iterator : result) {
			System.out.println(iterator);
		}
	}

	@Test
	public void TestSelectO() {

		ArrayList<List<String>> result = select(2, "UK_Independence_Party");
		for (List<String> iterator : result) {
			System.out.println(iterator);
		}
	}

	@Test
	public void TestSelectPP() {

		String p1 = "isLeaderOf";
		String p2 = "isCitizenOf";
		ArrayList<List<String>> result1 = select(1, p1);
		ArrayList<List<String>> result2 = select(1, p2);

		HashSet<String> commonO = new HashSet<String>();
		for (List<String> iterator1 : result1) {
			for (List<String> iterator2 : result2) {
				if (iterator1.get(0).equals(iterator2.get(0)))
					commonO.add(iterator1.get(0));
			}
		}
		for (String o : commonO) {
			System.out.println(o);
		}
	}

	@Test
	public void TestHasMostO() {

		String o = "UK_Independence_Party";
		ArrayList<List<String>> result = select(2, o);

		HashMap<String, Integer> occur_time = new HashMap<String, Integer>();
		for (List<String> item : result) {
			String s = item.get(0);
			if (occur_time.containsKey(s)) {
				occur_time.put(s, (Integer) (occur_time.get(s)) + 1);
			} else {
				occur_time.put(s, 1);
			}
		}
		List<Map.Entry<String, Integer>> list = new ArrayList<Map.Entry<String, Integer>>(occur_time.entrySet());
		Collections.sort(list, new Comparator<Map.Entry<String, Integer>>() {
			@Override
			public int compare(Map.Entry<String, Integer> kv1, Map.Entry<String, Integer> kv2) {
				return kv2.getValue().compareTo(kv1.getValue());
			}
		});
		int maxTime = list.get(0).getValue();
		for (Map.Entry<String, Integer> keypair : list) {
			if (keypair.getValue() == maxTime) {
				System.out.println(keypair.getKey());
			} else {
				break;
			}
		}
	}

	private ArrayList<List<String>> select(int field, String P) {
		FindIterable<Document> findIterable = collection.find(Filters.eq(columns[field], P));
		MongoCursor<Document> mongoCursor = findIterable.iterator();
		ArrayList<List<String>> result = new ArrayList<List<String>>();

		while (mongoCursor.hasNext()) {
			Document doc = mongoCursor.next();
			List<String> item = new ArrayList<String>();
			for (String col : columns) {
				item.add((String) doc.get(col));
			}
			result.add(item);
		}
		return result;
	}

}