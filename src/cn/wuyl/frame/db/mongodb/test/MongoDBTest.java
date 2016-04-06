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
			conn=new Mongo(HOST,PORT);//�������ݿ�����
			myDB=conn.getDB(DB_NAME);//ʹ��test���ݿ�
//			boolean loginSuccess=myDB.authenticate(USER, PASSWORD.toCharArray());//�û���֤
//			if(loginSuccess){
				myCollection=myDB.getCollection(COLLECTION);
//			}
		} catch (MongoException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * ���¡�������
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
		 * updateCondition:��������
		 * updateSetValue:���õ���ֵ
		 */
		collection.update(updateCondition, updateSetValue);
		
		DBObject queryCondition=new BasicDBObject();
		
		//where name='sam',�������ڸ���ǰ���ǳ�����
		queryCondition.put("name", "sam");
		
		DBObject setValue=new BasicDBObject();
		setValue.put("headers", 1);
		setValue.put("legs", 1);
		
		DBObject upsertValue=new BasicDBObject("$set",setValue);
		/**
		 * ����������������ֱ��ǣ�
		 * �������µ�����û�У������
		 * ��ͬʱ���¶�������������ĵ�(collection)
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
	 * ���ز�ѯ�����
	 * @param collection
	 * @return
	 */
	private static DBCursor queryData(DBCollection collection){
		DBCursor queriedData=collection.find();
		return queriedData;
		
	}
	
	/**
	 * ��ӡ�������
	 * @param description����������������
	 * @param recordResult�������
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
	
		printData("�鿴һ�¸���ǰ�����ݣ�",queryData(myCollection));
		//��������
		//updateData(myCollection);
		//printData("�鿴һ�¸��º�����ݣ�",queryData(myCollection));
		
	}

}
