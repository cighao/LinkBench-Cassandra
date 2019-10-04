package com.facebook.LinkBench;


import com.datastax.driver.core.exceptions.*;
import com.datastax.driver.core.*;

import java.io.IOException;
import java.util.*;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class LinkStoreCassandra extends GraphStore {
    public static final String CONFIG_HOST = "host";
    public static final String CONFIG_PORT = "port";
    public static final int RETRY_NUM = 3;
    public static final int DEFAULT_BULKINSERT_SIZE = 40;

    private static Session cql_session;
    private static Cluster cluster;

    private static String linktable;
    private static String counttable;
    private static String nodetable;
    private static String host;
    private static String port;
    private static String defaultKeySpace;

    private static Level debuglevel;
    private static  Phase phase;
    private static int totalThreads = 0;

    private int bulkInsertSize = DEFAULT_BULKINSERT_SIZE;
    private final Logger logger = Logger.getLogger(ConfigUtil.LINKBENCH_LOGGER);

    public LinkStoreCassandra() {
        super();
    }

    public void initialize(Properties props, Phase currentPhase,
                           int threadId) throws IOException, Exception {
        counttable = ConfigUtil.getPropertyRequired(props, Config.COUNT_TABLE);
        if (counttable == null || counttable.equals(""))
            counttable = "counttable";
        nodetable = props.getProperty(Config.NODE_TABLE);
        if (nodetable == null || nodetable.equals(""))
            nodetable = "nodetable";
        linktable = ConfigUtil.getPropertyRequired(props, Config.LINK_TABLE);
        if (linktable == null || linktable.equals(""))
            linktable = "counttable";
        host = ConfigUtil.getPropertyRequired(props, CONFIG_HOST);
        if (host == null || host.equals(""))
            host = "127.0.0.1";
        port = props.getProperty(CONFIG_PORT);
        if (port == null || port.equals(""))
            port = "9042";
        defaultKeySpace = ConfigUtil.getPropertyRequired(props, Config.DBID);
        debuglevel = ConfigUtil.getDebugLevel(props);
        phase = currentPhase;
        try {
            cassandra_init();
        } catch (Exception e) {
            logger.error("error connecting to database:", e);
            throw e;
        }
    }

    static synchronized void cassandra_init(){
        if (++totalThreads > 1)
            return ;
        try{
            assert(cluster == null);
            assert(cql_session == null);
            cluster = Cluster.builder().withPort(Integer.parseInt(port)).addContactPoint(host).build();
            cql_session = cluster.connect(defaultKeySpace);
            if(phase == Phase.LOAD){
                String clean_node = "truncate table " + defaultKeySpace + "." + nodetable + "";
                cql_session.execute(clean_node);
                String clean_link = "truncate table " + defaultKeySpace + "." + linktable + "";
                cql_session.execute(clean_link);
            }
        }catch (DriverException e){
            throw e;
        }
    }

    static synchronized void close_cassandra() {
        if (--totalThreads > 0) {
            return ;
        }
        try{
            assert(cql_session != null);
            assert(cluster != null);
            cql_session.close();
            cluster.close();
        }catch (DriverException e){
            throw e;
        }
    }

    @Override
    public void close() {
        close_cassandra();
    }

    @Override
    public void clearErrors(int threadID) {
        try{
            cql_session.close();
            cluster.close();
            cassandra_init();
        }catch (Throwable e) {
            e.printStackTrace();
            return;
        }
    }

    @Override
    public boolean addLink(String dbid, Link link, boolean noinverse) throws Exception {
        int retry_num = RETRY_NUM;
        while (true) {
            try {
                return addLinkImpl(dbid, link, noinverse);
            } catch (DriverException e) {
                retry_num--;
                if(retry_num <= 0){
                    throw  e;
                }
            }
        }
    }

    private boolean addLinkImpl(String dbid, Link link, boolean noinverse)
            throws Exception {
        if (Level.DEBUG.isGreaterOrEqual(debuglevel)) {
            logger.debug("addLink " + link.id1 + "." + link.id2 + "." + link.link_type);
        }
//        String query = "select * from " + dbid + "." + linktable
//                       + " where id1 = " + link.id1 + " and id2 = "
//                       + link.id2 + " and link_type = " + link.link_type + ";";
//        ResultSet rs = cql_session.execute(query);
//        Row row = rs.one();
        boolean is_update = false;
//        if(row != null){
//            is_update = true;
//        }
        String insert = "INSERT INTO " + dbid + "." + linktable +  "(id1, id2, link_type, "
                + "visibility, data, time, version) VALUES ("+ link.id1 + "," + link.id2
                + "," + link.link_type + "," + link.visibility + ",'" + link.data + "',"
                + link.time + "," + link.version + ")";
        cql_session.execute(insert);
        return is_update;
    }

    @Override
    public boolean deleteLink(String dbid, long id1, long link_type, long id2, boolean noinverse, boolean expunge) throws Exception {
        int retry_num = RETRY_NUM;
        while (true) {
            try {
                return deleteLinkImpl(dbid, id1, link_type, id2, noinverse, expunge);
            } catch (DriverException e) {
                retry_num--;
                if(retry_num <= 0){
                    throw  e;
                }
            }
        }
    }

    private boolean deleteLinkImpl(String dbid, long id1, long link_type,
            long id2, boolean noinverse, boolean expunge) throws Exception {
        if (Level.DEBUG.isGreaterOrEqual(debuglevel)) {
            logger.debug("deleteLink " + id1 + "." + id2 + "." + link_type);
        }
        String query = "SELECT visibility FROM " + dbid + "." + linktable +
                       " WHERE id1 = " + id1 + " AND id2 = " + id2 +
                       " AND link_type = " + link_type + "";
        ResultSet rs = cql_session.execute(query);

        int visibility = -1;
        boolean found = false;
        for(Row row : rs){
            visibility = row.getInt("visibility");
            found = true;
        }
        if(!found){
            // do nothing
        }else if(visibility == VISIBILITY_HIDDEN && !expunge){
            // do nothing
        }else{
            // only update visible
            String delete;
            if(!expunge){
                delete = "UPDATE " + dbid + "." + linktable +
                        " SET visibility = " + VISIBILITY_HIDDEN +
                        " WHERE id1 = " + id1 +
                        " AND id2 = " + id2 +
                        " AND link_type = " + link_type + ";";
            }else{
                delete = "DELETE FROM " + dbid + "." + linktable +
                        " WHERE id1 = " + id1 +
                        " AND id2 = " + id2 +
                        " AND link_type = " + link_type + ";";
            }
            if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
                logger.trace(delete);
            }
            System.out.println(delete);
            cql_session.execute(delete);
        }
        return found;
    }

    @Override
    public boolean updateLink(String dbid, Link link, boolean noinverse) throws Exception {
        boolean added = addLink(dbid, link, noinverse);
        return !added; // return true if updated instead of added
    }

    @Override
    public Link getLink(String dbid, long id1, long link_type, long id2) throws Exception {
        int retry_num = RETRY_NUM;
        while (true) {
            try {
                return getLinkImpl(dbid, id1, link_type, id2);
            } catch (DriverException e) {
                retry_num--;
                if(retry_num <= 0){
                    throw  e;
                }
            }
        }
    }

    private Link getLinkImpl(String dbid, long id1, long link_type, long id2)
            throws Exception {
        Link res[] = multigetLinks(dbid, id1, link_type, new long[] {id2});
        if (res == null)
            return null;
        assert(res.length <= 1);
        return res.length == 0 ? null : res[0];
    }

    @Override
    public Link[] multigetLinks(String dbid, long id1, long link_type, long[] id2s) throws Exception {
        int retry_num = RETRY_NUM;
        while (true) {
            try {
                return multigetLinksImpl(dbid, id1, link_type, id2s);
            } catch (DriverException e) {
                retry_num--;
                if(retry_num <= 0){
                    throw  e;
                }
            }
        }
    }

    private Link[] multigetLinksImpl(String dbid, long id1, long link_type,
                                     long[] id2s) throws Exception {
        StringBuilder querySB = new StringBuilder();
        querySB.append(" select id1, id2, link_type," +
                " visibility, data, time, " +
                " version from " + dbid + "." + linktable +
                " where id1 = " + id1 + " and link_type = " + link_type +
                " and id2 in (");
        boolean first = true;
        for (long id2: id2s) {
            if (first) {
                first = false;
            } else {
                querySB.append(",");
            }
            querySB.append(id2);
        }
        querySB.append(");");
        String query = querySB.toString();

        if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
            logger.trace("Query is " + query);
        }
        ResultSet rs = cql_session.execute(query);
        List<Row> rows = rs.all();
        int size = rows.size();
        if(size == 0)
            return null;
        Link results[] = new Link[size];
        int i = 0;
        for(Row row : rows){
            Link link = createLinkFromRow(row);
            if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
                logger.trace("Lookup result: " + id1 + "," + link_type + "," +  link.id2 + " found");
            }
            results[i++] = link;
        }
        return results;
    }

    @Override
    public Link[] getLinkList(String dbid, long id1, long link_type) throws Exception {
        int retry_num = RETRY_NUM;
        while (true) {
            try {
                return getLinkList(dbid, id1, link_type, 0, Long.MAX_VALUE, 0, rangeLimit);
            } catch (DriverException e) {
                retry_num--;
                if(retry_num <= 0){
                    throw  e;
                }
            }
        }
    }

    @Override
    public Link[] getLinkList(String dbid, long id1, long link_type,
                                     long minTimestamp, long maxTimestamp,
                                     int offset, int limit)throws Exception {
        String query = " select id1, id2, link_type," +
                " visibility, data, time, version from " + dbid + "." + linktable +
                " where id1 = " + id1 + " and link_type = " + link_type +
                " and time >= " + minTimestamp +
                " and time <= " + maxTimestamp +
                " and visibility = " + LinkStore.VISIBILITY_DEFAULT +
                " ALLOW FILTERING";
        if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
            logger.trace("Query is " + query);
        }
        ResultSet rs = cql_session.execute(query);
        List<Row> rows = rs.all();
        int size = rows.size();
        if(size == 0)
            return null;
        Link results[] = new Link[size];
        int i = 0;
        for(Row row : rows){
            Link link = createLinkFromRow(row);
            results[i++] = link;
        }
        return results;
    }

    @Override
    public long countLinks(String dbid, long id1, long link_type) throws Exception {
        int retry_num = RETRY_NUM;
        while (true) {
            try {
                return countLinksImpl(dbid, id1, link_type);
            } catch (DriverException e) {
                retry_num--;
                if(retry_num <= 0){
                    throw  e;
                }
            }
        }
    }

    private long countLinksImpl(String dbid, long id1, long link_type)
            throws Exception {
        long count = 0;
        String query = "select count(*) from " + dbid + "." + linktable +
                " where id1 = " + id1 + " and link_type = " + link_type + " ALLOW FILTERING;";
        ResultSet rs = cql_session.execute(query);
        return rs.one().getLong("count");
    }

    @Override
    public int bulkLoadBatchSize() {
        return bulkInsertSize;
    }

    @Override
    public void addBulkLinks(String dbid, List<Link> links, boolean noinverse) throws Exception {
        int retry_num = RETRY_NUM;
        while (true) {
            try {
                addBulkLinksImpl(dbid, links, noinverse);
                return;
            } catch (DriverException e) {
                retry_num--;
                if(retry_num <= 0){
                    throw  e;
                }
            }
        }
    }

    private void addBulkLinksImpl(String dbid, List<Link> links, boolean noinverse) throws Exception {
        if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
            logger.trace("addBulkLinks: " + links.size() + " links");
        }
        if (links.size() == 0)
            return ;
        String query = "INSERT INTO " + dbid + "." + linktable +  "(id1, id2, " +
                "link_type, visibility, data, time, version) VALUES (?,?,?,?,?,?,?)";
        PreparedStatement prepareStatement = cql_session.prepare(query);
        BatchStatement bs = new BatchStatement();
        for(Link link : links) {
            Statement s = prepareStatement.bind(link.id1, link.id2, link.link_type,
                    (int)link.visibility, link.data.toString(), link.time, link.version);
            bs.add(s);
        }
        if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
            logger.trace("INSERT INTO " + dbid + "." + linktable);
        }
        cql_session.execute(bs);

        /* this is for driver-4.0.0 API */
        /* PreparedStatement prepareStatement = cql_session.prepare(query);
        BatchStatementBuilder bsb = BatchStatement.builder(BatchType.LOGGED);
        for(Link link : links) {
            BoundStatement bs = prepareStatement.bind(link.id1, link.id2, link.link_type,
                    (int)link.visibility, link.data.toString(), link.time, link.version);
            bsb.addStatement(bs);
        }
        BatchStatement batchStatement = bsb.build();
        if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
            logger.trace("INSERT INTO " + dbid + "." + linktable);
        }
        cql_session.execute(batchStatement); */
    }


    @Override
    public void addBulkCounts(String dbid, List<LinkCount> a) throws Exception {
        // do nothing
    }

    @Override
    public void resetNodeStore(String dbid, long startID) throws Exception {
        String truncate = "TRUNCATE  " + dbid + "." + nodetable + ";";
        cql_session.execute(truncate);
    }

    @Override
    public long addNode(String dbid, Node node) throws Exception {
        int retry_num = RETRY_NUM;
        while (true) {
            try {
                return addNodeImpl(dbid, node);
            } catch (DriverException e) {
                retry_num--;
                if(retry_num <= 0){
                    throw  e;
                }
            }
        }
    }

    private long addNodeImpl(String dbid, Node node) throws Exception {
        long ids[] = bulkAddNodes(dbid, Collections.singletonList(node));
        assert(ids.length == 1);
        return ids[0];
    }


    @Override
    public long[] bulkAddNodes(String dbid, List<Node> nodes) throws Exception {
        int retry_num = RETRY_NUM;
        while (true) {
            try {
                return bulkAddNodesImpl(dbid, nodes);
            } catch (DriverException e) {
                retry_num--;
                if(retry_num <= 0){
                    throw  e;
                }
            }
        }
    }

    private long[] bulkAddNodesImpl(String dbid, List<Node> nodes){
        assert(nodes.size() > 0);
        long IDs[] = new long[nodes.size()];
        PreparedStatement prepareStatement = cql_session.prepare("INSERT INTO "
                + dbid + "." + nodetable +  "(id, type, version, time, data) VALUES (?,?,?,?,?)");
        BatchStatement bs = new BatchStatement();
        int i = 0;
        for(Node node : nodes) {
            Statement s = prepareStatement.bind(node.id, node.type,
                                node.version, node.time, node.data.toString());
            bs.add(bs);
            IDs[i++] = node.id;
        }
        assert(i == nodes.size());
        if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
            logger.trace("INSERT INTO " + dbid + "." + linktable);
        }
        cql_session.execute(bs);

        /* this is for driver-4.0.0 API */
        /*BatchStatementBuilder bsb = BatchStatement.builder(BatchType.LOGGED);
        int i = 0;
        for(Node node : nodes) {
            BoundStatement bs = prepareStatement.bind(node.id, node.type,
                                node.version, node.time, node.data.toString());
            bsb.addStatement(bs);
            IDs[i++] = node.id;
        }
        assert(i == nodes.size());
        BatchStatement batchStatement = bsb.build();
        cql_session.execute(batchStatement);*/
        return IDs;
    }

    @Override
    public Node getNode(String dbid, int type, long id) throws Exception {
        int retry_num = RETRY_NUM;
        while (true) {
            try {
                return getNodeImpl(dbid, type, id);
            } catch (DriverException e) {
                retry_num--;
                if(retry_num <= 0){
                    throw  e;
                }
            }
        }
    }

    private Node getNodeImpl(String dbid, int type, long id) throws Exception {
        String query = "SELECT id, type, version, time, data " +
                "FROM " + dbid + "." + nodetable + " WHERE id= "
                 + id + " and type = " + type + " allow filtering;";
        ResultSet rs = cql_session.execute(query);
        Row row = rs.one();
        if(row == null){
            return null;
        }else{
            return createNodeFromRow(row);
        }
    }

    @Override
    public boolean updateNode(String dbid, Node node) throws Exception {
        int retry_num = RETRY_NUM;
        while (true) {
            try {
                return updateNodeImpl(dbid, node);
            } catch (DriverException e) {
                retry_num--;
                if(retry_num <= 0){
                    throw  e;
                }
            }
        }
    }

    private boolean updateNodeImpl(String dbid, Node node) throws Exception {
        // we don't check the type here, because id is the unique primary key
        String update = "UPDATE " + dbid + "." + nodetable + " SET "
                + "version = " + node.version + ", time = " + node.time
                + ", data = '" + node.data.toString() + "' WHERE id = "
                + node.id + " if exists";
        if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
            logger.trace(update);
        }
        ResultSet rs = cql_session.execute(update);
        if(rs.wasApplied()){ // update
            return true;
        }else{  // need to insert
            addNodeImpl(dbid, node);
            return false;
        }
    }

    @Override
    public boolean deleteNode(String dbid, int type, long id) throws Exception {
        int retry_num = RETRY_NUM;
        while (true) {
            try {
                return deleteNodeImpl(dbid, type, id);
            } catch (DriverException e) {
                retry_num--;
                if(retry_num <= 0){
                    throw  e;
                }
            }
        }
    }

    private boolean deleteNodeImpl(String dbid, int type, long id) throws Exception {
        // we don't use the type for filtering, because id is primary key
        String delete = "DELETE FROM " + dbid + "." + nodetable +
                " WHERE id = " + id + " if exists";
        ResultSet rs = cql_session.execute(delete);
        return rs.wasApplied();
    }

    private Link createLinkFromRow(Row row) {
        Link link = new Link();
        link.id1 = row.getLong("id1");
        link.id2 = row.getLong("id2");
        link.link_type = row.getLong("link_type");
        link.visibility = (byte) row.getInt("visibility");
        link.data = row.getString("data").getBytes();
        link.time = row.getLong("time");
        link.version = row.getInt("version");
        return link;
    }

    private Node createNodeFromRow(Row row) {
        Node node = new Node(row.getLong("id"),
                             row.getInt("type"),
                             row.getLong("version"),
                             row.getInt("time"),
                             row.getString("data").getBytes());
        return node;
    }

    public static void main(String[] args){
        /*System.out.println("hello cassandra");
        CqlSession session = CqlSession.builder().build();
        ResultSet rs = session.execute("select release_version from system.local");
        Row row = rs.one();
        System.out.println(row.getString("release_version"));*/
        LinkStoreCassandra s = new LinkStoreCassandra();
        s.test();
    }

    private void test(){

        // 0. prepare
        cluster = Cluster.builder().withPort(9042).addContactPoint("127.0.0.1").build();
        cql_session = cluster.connect(defaultKeySpace);
        linktable = "linktable";
        nodetable = "nodetable";
        String bdid = "linkdb";


        //1. add link
        /*Link link1 = new Link(1,2,3, Byte.parseByte("1"),"aaaa".getBytes(),1,100);
        try{
            addLink(bdid, link1, true);
        }catch (Exception e){
            System.out.println("insert link failed");
        }*/

        //2. delete link
        /*try{
            deleteLink(bdid, 1,2,3,true,true);
        }catch (Exception e){
            System.out.println("delete link failed");
        }*/

        //3. get link
        /*try{
            Link link2 = getLink(bdid, 1,2,3);
            System.out.println(link2.toString());
        }catch (Exception e){
            System.out.println("delete link failed");
        }*/

        //4. count links
        try{
            Long r = countLinks(bdid, 1,2);
            System.out.println(r);
        }catch (Exception e){
            System.out.println("count links failed");
        }

        // 5. add bulk links
        /*List<Link> links = new ArrayList<>();
        links.add(new Link(1,2,3, Byte.parseByte("1"),"aaaa".getBytes(),1,100));
        links.add(new Link(1,4,3, Byte.parseByte("1"),"aaaa".getBytes(),1,100));
        links.add(new Link(1,2,5, Byte.parseByte("1"),"aaaa".getBytes(),1,100));
        System.out.println(links.size());
        try{
            addBulkLinks(bdid, links, true);
        }catch (Exception e){
            System.out.println("add bulk links failed");
        }*/

        // 6. add node
        /*Node node1 = new Node(2,1,10,1000,"aaa".getBytes());
        try{
            Long r = addNode(bdid,node1);
            System.out.println(r);
        }catch (Exception e){
            System.out.println("update node failed");
        }*/

        // 7. get node
        /*try{
            Node node2 = getNode(bdid, 1,1);
            System.out.println(node2.toString());
        }catch (Exception e){
            System.out.println("get node failed");
        }*/

        // 8. update node
        /*try{
            Node node3 = new Node(1,1,10,100,"aaa".getBytes());
            updateNode(bdid, node3);
        }catch (Exception e){
            System.out.println("update node failed");
        }*/

        // 9. delete node
        /*try{
            deleteNode(bdid, 2,1);
        }catch (Exception e){
            System.out.println("delete node failed");
        }*/

        // 10. reset
        /*try{
            resetNodeStore(bdid,0);
            resetLinkStore(bdid);
        }catch (Exception e){
            System.out.println("reset failed");
        }*/

        // 11.
        /*Random random = new Random();
        Long start = System.nanoTime();
        for(int i=0;i<10000;i++){
            int a = random.nextInt();
            Node tmp = new Node(a,1,10,1000,"aaa".getBytes());
            try{
                Long r = addNode(bdid,tmp);
            }catch (Exception e){
                System.out.println("update node failed");
            }
        }
        Long end = System.nanoTime();
        System.out.println("time: " + (end-start)/1000000.0 + " ms");*/

        cql_session.close();
        cluster.close();
    }
}