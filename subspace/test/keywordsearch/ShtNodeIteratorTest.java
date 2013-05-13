package keywordsearch;

import junit.framework.Assert;
import keywordsearch.backward.ShortestNodeIterator;
import keywordsearch.backward.Neo4jKeywordSearch.DjskState;

import org.junit.Test;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.EmbeddedGraphDatabase;

import util.Constant;

public class ShtNodeIteratorTest {

	public enum Relation implements RelationshipType {
		test
	}

	/**
	 * 测试两个iterator同时访问同一条路径时，这条路径能够被访问两次
	 */
	@Test
	public void visitCommPathTest() {
		EmbeddedGraphDatabase graphDb = new EmbeddedGraphDatabase("test.db");
		Transaction tnx = graphDb.beginTx();
		Node pp = graphDb.createNode();
		pp.setProperty("post", "hello calculated");
		
		Node p = graphDb.createNode();
		p.setProperty("post", "tree calculated");
		Relationship ship = p.createRelationshipTo(pp, Relation.test);
		ship.setProperty(Constant.WEIGHT, 1.0f);
		
		Node c1 = graphDb.createNode();
		c1.setProperty("post", "Prim algorithm");
		Node c2 = graphDb.createNode();
		c2.setProperty("post", "includes destination");

		ship = c1.createRelationshipTo(p, Relation.test);
		ship.setProperty(Constant.WEIGHT, 1.0f);
		ship = c2.createRelationshipTo(p, Relation.test);
		ship.setProperty(Constant.WEIGHT, 1.0f);
		
		Node c3 = graphDb.createNode();
		c3.setProperty("post", "which means");
		Node c4 = graphDb.createNode();
		c4.setProperty("post", "Steiner tree");
		Node c5 = graphDb.createNode();
		c5.setProperty("post", "spanning tree");
		ship = c3.createRelationshipTo(c2, Relation.test);
		ship.setProperty(Constant.WEIGHT, 1.0f);
		ship = c4.createRelationshipTo(c2, Relation.test);
		ship.setProperty(Constant.WEIGHT, 1.0f);
		ship = c5.createRelationshipTo(c1, Relation.test);
		ship.setProperty(Constant.WEIGHT, 1.0f);
		tnx.success();
		tnx.finish();

		DjskState state = new DjskState();
		ShortestNodeIterator iter1 = new ShortestNodeIterator(c3.getId(),
				graphDb, state, Relation.test, 8);
		ShortestNodeIterator iter2 = new ShortestNodeIterator(c4.getId(),
				graphDb, state, Relation.test, 8);

		Assert.assertEquals(c2.getId(), iter1.next().arg0.longValue());
		Assert.assertEquals(c2.getId(), iter2.next().arg0.longValue());

		Assert.assertEquals(p.getId(), iter1.next().arg0.longValue());
		System.out.println(iter2.hasNext());
		Assert.assertTrue(iter2.hasNext());
		iter2.next();
		Assert.assertFalse(iter1.hasNext());
		Assert.assertFalse(iter2.hasNext());
	}
	
	/**
	 * 测试shortestIter确实是按照距离的顺序返回结果
	 */
	@Test
	public void shortestPathTest(){
		EmbeddedGraphDatabase graphDb = new EmbeddedGraphDatabase("test1.db");
		Transaction tnx = graphDb.beginTx();

		Node p = graphDb.createNode();
		p.setProperty("post", "tree calculated");
		Node c1 = graphDb.createNode();
		c1.setProperty("post", "Prim algorithm");
		Node c2 = graphDb.createNode();
		c2.setProperty("post", "includes destination");

		Relationship rel = c1.createRelationshipTo(p, Relation.test);
		rel.setProperty(Constant.WEIGHT, 3.0f);
		rel = c2.createRelationshipTo(p, Relation.test);
		rel.setProperty(Constant.WEIGHT, 1.0f);
		
		Node c3 = graphDb.createNode();
		c3.setProperty("post", "which means");
		Node c4 = graphDb.createNode();
		c4.setProperty("post", "Steiner tree");
		Node c5 = graphDb.createNode();
		c5.setProperty("post", "spanning tree");
		rel = c3.createRelationshipTo(c2, Relation.test);
		rel.setProperty(Constant.WEIGHT, 1.0f);
		rel = c3.createRelationshipTo(c4, Relation.test);
		rel.setProperty(Constant.WEIGHT, 1.0f);
		rel = c3.createRelationshipTo(c5, Relation.test);
		rel.setProperty(Constant.WEIGHT, 1.0f);
		rel = c5.createRelationshipTo(c1, Relation.test);
		rel.setProperty(Constant.WEIGHT, 1.0f);
		
		/**
		 *      p____3____|  
		 *      |         c1
		 *     c2    c4   c5   
		 *           c3
		 *     p应该从路径c3->c2->p的路径返回
		 */
		tnx.success();
		tnx.finish();
		
		DjskState state = new DjskState();
		ShortestNodeIterator iter1 = new ShortestNodeIterator(c3.getId(),
				graphDb, state, Relation.test, 8);
		
		while (iter1.hasNext()) {
			System.out.println(iter1.next());
		}
	}
}
