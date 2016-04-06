package cn.wuyl.frame.db.mongodb.test;

import java.net.UnknownHostException;
import java.util.Iterator;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoException;

public class MongoDBTest {

	private static final String HOST = "127.0.0.1";
	private static final int PORT = 27017;
	private static final String USER = "admin";
	private static final String PASSWORD = "admin";
	private static final String DB_NAME = "admin";
	private static final String COLLECTION = "system.users";
	private static Mongo conn=null;
	private static DB myDB=null;
	private static DBCollection myCollection=null;
	
	static{
		try {
			conn=new Mongo(HOST,PORT);//建立数据库连接
			myDB=conn.getDB(DB_NAME);//使用test数据库
//			boolean loginSuccess=myDB.authenticate(USER, PASSWORD.toCharArray());//用户验证
//			if(loginSuccess){
				myCollection=myDB.getCollection(COLLECTION);
//			}
		} catch (MongoException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 更新“表”数据
	 * @param collection
	 */
	private static void updateData(DBCollection collection){
		DBObject updateCondition=new BasicDBObject();
		
		//where name='fox'
		updateCondition.put("name", "fox");
		
		DBObject updatedValue=new BasicDBObject();
		updatedValue.put("headers", 3);
		updatedValue.put("legs", 4);
		
		DBObject updateSetValue=new BasicDBObject("$set",updatedValue);
		/**
		 * update insert_test set headers=3 and legs=4 where name='fox'
		 * updateCondition:更新条件
		 * updateSetValue:设置的新值
		 */
		collection.update(updateCondition, updateSetValue);
		
		DBObject queryCondition=new BasicDBObject();
		
		//where name='sam',此条件在更新前不是成立的
		queryCondition.put("name", "sam");
		
		DBObject setValue=new BasicDBObject();
		setValue.put("headers", 1);
		setValue.put("legs", 1);
		
		DBObject upsertValue=new BasicDBObject("$set",setValue);
		/**
		 * 后面两个参数含义分别是：
		 * 若所更新的数据没有，则插入
		 * ，同时更新多个符合条件的文档(collection)
		 */
		collection.update(queryCondition, upsertValue, true, true);
		//set headers=headers+2
		DBObject incValue=new BasicDBObject("headers",2);
		//set legs=4
		DBObject legsValue=new BasicDBObject("legs",4);
		
		DBObject allCondition=new BasicDBObject();
		allCondition.put("$inc", incValue);
		allCondition.put("$set", legsValue);
		
		collection.update(queryCondition, allCondition);
	}
	
	/**
	 * 返回查询结果集
	 * @param collection
	 * @return
	 */
	private static DBCursor queryData(DBCollection collection){
		DBCursor queriedData=collection.find();
		return queriedData;
		
	}
	
	/**
	 * 打印结果数据
	 * @param description　结果数据相关描述
	 * @param recordResult　结果集
	 */
	private static void printData(String description,DBCursor recordResult){
		System.out.println(description);
		for(Iterator<DBObject> iter=recordResult.iterator();iter.hasNext();){
			System.out.println(iter.next());
		}
	}
	
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
	
		printData("查看一下更新前的数据：",queryData(myCollection));
		//更新数据
		//updateData(myCollection);
		//printData("查看一下更新后的数据：",queryData(myCollection));
		
	}

}
