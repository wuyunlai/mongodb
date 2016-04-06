package cn.wuyl.frame.db.mongodb.util;
import java.util.ArrayList;
import java.util.List;

//import org.apache.commons.configuration.CompositeConfiguration;
//import org.apache.commons.configuration.ConfigurationException;
//import org.apache.commons.configuration.PropertiesConfiguration;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoClientOptions.Builder;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.model.Filters;
import com.mongodb.client.result.DeleteResult;

/**
 * MongoDB������ Mongoʵ��������һ�����ݿ����ӳأ���ʹ�ڶ��̵߳Ļ����У�һ��Mongoʵ����������˵�Ѿ��㹻��<br>
 * ע��Mongo�Ѿ�ʵ�������ӳأ��������̰߳�ȫ�ġ� <br>
 * ���Ϊ����ģʽ�� �� MongoDB��Java�������̰߳�ȫ�ģ�����һ���Ӧ�ã�ֻҪһ��Mongoʵ�����ɣ�<br>
 * Mongo�и����õ����ӳأ�Ĭ��Ϊ10���� �����д���д�Ͷ��Ļ����У�Ϊ��ȷ����һ��Session��ʹ��ͬһ��DBʱ��<br>
 * DB��DBCollection�Ǿ����̰߳�ȫ��<br>
 * 
 * @author zhoulingfei
 * @date 2015-5-29 ����11:49:49
 * @version 0.0.0
 * @Copyright (c)1997-2015 NavInfo Co.Ltd. All Rights Reserved.
 */
public enum MongoDBUtil {

    /**
     * ����һ��ö�ٵ�Ԫ�أ�����������һ��ʵ��
     */
    instance;

    private MongoClient mongoClient;

    static {
        System.out.println("===============MongoDBUtil��ʼ��========================");
//        CompositeConfiguration config = new CompositeConfiguration();
//        try {
//            config.addConfiguration(new PropertiesConfiguration("mongodb.properties"));
//        } catch (ConfigurationException e) {
//            e.printStackTrace();
//        }
        // �������ļ��л�ȡ����ֵ
        String ip = "127.0.0.1";//config.getString("host");
        int port = 27017;//config.getInt("port");
        instance.mongoClient = new MongoClient(ip, port);

        // or, to connect to a replica set, with auto-discovery of the primary, supply a seed list of members
        // List<ServerAddress> listHost = Arrays.asList(new ServerAddress("localhost", 27017),new ServerAddress("localhost", 27018));
        // instance.mongoClient = new MongoClient(listHost);

        // �󲿷��û�ʹ��mongodb���ڰ�ȫ�����£��������mongodb��Ϊ��ȫ��֤ģʽ������Ҫ�ڿͻ����ṩ�û��������룺
        // boolean auth = db.authenticate(myUserName, myPassword);
        Builder options = new MongoClientOptions.Builder();
        // options.autoConnectRetry(true);// �Զ�����true
        // options.maxAutoConnectRetryTime(10); // the maximum auto connect retry time
        options.connectionsPerHost(300);// ���ӳ�����Ϊ300������,Ĭ��Ϊ100
        options.connectTimeout(15000);// ���ӳ�ʱ���Ƽ�>3000����
        options.maxWaitTime(5000); //
        options.socketTimeout(0);// �׽��ֳ�ʱʱ�䣬0������
        options.threadsAllowedToBlockForConnectionMultiplier(5000);// �̶߳���������������߳������˶��оͻ��׳���Out of semaphores to get db������
        options.writeConcern(WriteConcern.SAFE);//
        options.build();
    }

    // ------------------------------------���÷���---------------------------------------------------
    /**
     * ��ȡDBʵ�� - ָ��DB
     * 
     * @param dbName
     * @return
     */
    public MongoDatabase getDB(String dbName) {
        if (dbName != null && !"".equals(dbName)) {
            MongoDatabase database = mongoClient.getDatabase(dbName);
            return database;
        }
        return null;
    }

    /**
     * ��ȡcollection���� - ָ��Collection
     * 
     * @param collName
     * @return
     */
    public MongoCollection<Document> getCollection(String dbName, String collName) {
        if (null == collName || "".equals(collName)) {
            return null;
        }
        if (null == dbName || "".equals(dbName)) {
            return null;
        }
        MongoCollection<Document> collection = mongoClient.getDatabase(dbName).getCollection(collName);
        return collection;
    }

    /**
     * ��ѯDB�µ����б���
     */
    public List<String> getAllCollections(String dbName) {
        MongoIterable<String> colls = getDB(dbName).listCollectionNames();
        List<String> _list = new ArrayList<String>();
        for (String s : colls) {
            _list.add(s);
        }
        return _list;
    }

    /**
     * ��ȡ�������ݿ������б�
     * 
     * @return
     */
    public MongoIterable<String> getAllDBNames() {
        MongoIterable<String> s = mongoClient.listDatabaseNames();
        return s;
    }

    /**
     * ɾ��һ�����ݿ�
     */
    public void dropDB(String dbName) {
        getDB(dbName).drop();
    }

    /**
     * ���Ҷ��� - ��������_id
     * 
     * @param collection
     * @param id
     * @return
     */
    public Document findById(MongoCollection<Document> coll, String id) {
        ObjectId _idobj = null;
        try {
            _idobj = new ObjectId(id);
        } catch (Exception e) {
            return null;
        }
        Document myDoc = coll.find(Filters.eq("_id", _idobj)).first();
        return myDoc;
    }

    /** ͳ���� */
    public int getCount(MongoCollection<Document> coll) {
        int count = (int) coll.count();
        return count;
    }

    /** ������ѯ */
    public MongoCursor<Document> find(MongoCollection<Document> coll, Bson filter) {
        return coll.find(filter).iterator();
    }

    /** ��ҳ��ѯ */
    public MongoCursor<Document> findByPage(MongoCollection<Document> coll, Bson filter, int pageNo, int pageSize) {
        Bson orderBy = new BasicDBObject("_id", 1);
        return coll.find(filter).sort(orderBy).skip((pageNo - 1) * pageSize).limit(pageSize).iterator();
    }

    /**
     * ͨ��IDɾ��
     * 
     * @param coll
     * @param id
     * @return
     */
    public int deleteById(MongoCollection<Document> coll, String id) {
        int count = 0;
        ObjectId _id = null;
        try {
            _id = new ObjectId(id);
        } catch (Exception e) {
            return 0;
        }
        Bson filter = Filters.eq("_id", _id);
        DeleteResult deleteResult = coll.deleteOne(filter);
        count = (int) deleteResult.getDeletedCount();
        return count;
    }

    /**
     * FIXME
     * 
     * @param coll
     * @param id
     * @param newdoc
     * @return
     */
    public Document updateById(MongoCollection<Document> coll, String id, Document newdoc) {
        ObjectId _idobj = null;
        try {
            _idobj = new ObjectId(id);
        } catch (Exception e) {
            return null;
        }
        Bson filter = Filters.eq("_id", _idobj);
        // coll.replaceOne(filter, newdoc); // ��ȫ���
        coll.updateOne(filter, new Document("$set", newdoc));
        return newdoc;
    }

    public void dropCollection(String dbName, String collName) {
        getDB(dbName).getCollection(collName).drop();
    }

    /**
     * �ر�Mongodb
     */
    public void close() {
        if (mongoClient != null) {
            mongoClient.close();
            mongoClient = null;
        }
    }

    /**
     * �������
     * 
     * @param args
     */
    public static void main(String[] args) {

        String dbName = "wuyl";
        String collName = "test";
        MongoCollection<Document> coll = MongoDBUtil.instance.getCollection(dbName, collName);

        // �������
         for (int i = 1; i <= 4; i++) {
         Document doc = new Document();
         doc.put("name", "zhoulf");
         doc.put("school", "NEFU" + i);
         Document interests = new Document();
         interests.put("game", "game" + i);
         interests.put("ball", "ball" + i);
         doc.put("interests", interests);
         coll.insertOne(doc);
         }

        // // ����ID��ѯ
         String id = "56e14950dcb8e8213c0d6fc8";
         Document doc = MongoDBUtil.instance.findById(coll, id);
         System.out.println(doc);

        // ��ѯ���
         MongoCursor<Document> cursor1 = coll.find(Filters.eq("name", "zhoulf")).iterator();
         while (cursor1.hasNext()) {
         org.bson.Document _doc = (Document) cursor1.next();
         System.out.println(_doc.toString());
         }
         cursor1.close();

        // ��ѯ���
//         MongoCursor<Person> cursor2 = coll.find(Person.class).iterator();

        // ɾ�����ݿ�
         //MongoDBUtil.instance.dropDB("wuyl");

        // ɾ����
         MongoDBUtil.instance.dropCollection(dbName, collName);

        // �޸�����
//         String id = "556949504711371c60601b5a";
         Document newdoc = new Document();
         newdoc.put("name", "ʱ��");
         MongoDBUtil.instance.updateById(coll, id, newdoc);

        // ͳ�Ʊ�
         System.out.println(MongoDBUtil.instance.getCount(coll));

        // ��ѯ����
        Bson filter = Filters.eq("count", 0);
        MongoDBUtil.instance.find(coll, filter);

    }

}
