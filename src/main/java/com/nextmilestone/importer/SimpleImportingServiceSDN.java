package com.nextmilestone.importer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.impl.util.FileUtils;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.data.neo4j.aspects.core.RelationshipBacked;
import org.springframework.data.neo4j.core.GraphDatabase;
import org.springframework.data.neo4j.support.DelegatingGraphDatabase;
import org.springframework.data.neo4j.support.Neo4jTemplate;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SimpleImportingServiceSDN {
	private static final Log log = LogFactory.getLog(SimpleImportingServiceSDN.class);
	private static final int NUMBER_OF_ORDERS = 1000;
	private static final int NUMBER_OF_ITEMS_PER_ORDER = 10;
	private static final String CONTAINS = "CONTAINS";
	private static Neo4jTemplate neo4jTemplate;

	public static void main(String[] args) throws IOException {
        FileUtils.deleteRecursively(new File("data/graph.db"));
		ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext("applicationContext.xml");
		neo4jTemplate = context.getBean("neo4jTemplate", Neo4jTemplate.class);

		importABunch();
	}

	private static void importABunch() {
		long startOfSave = now();
		importWithTransaction();
		log.info("Orders saved after: " + (now() - startOfSave));
	}

	private static void importWithTransaction() {
		Transaction transaction = neo4jTemplate.beginTx();
		try {
			importData();
			transaction.success();
		} finally {
			transaction.finish();
		}
	}

	private static void importData() {
		for (int orderid = 0; orderid < NUMBER_OF_ORDERS; orderid++) {
            final Order order = neo4jTemplate.save(new Order(String.valueOf(orderid)));
			List<Item> itemNodes = createItemNodes(generateItemsForOrder(NUMBER_OF_ITEMS_PER_ORDER, orderid));
			relateOrderToItems(order, itemNodes);
		}
	}

	private static List<Item> generateItemsForOrder(int numberOfItemsPerOrder, int orderId) {
		List<Item> items = new ArrayList<Item>(numberOfItemsPerOrder);
		for (int i = 0; i < numberOfItemsPerOrder; i++) {
			Item item = new Item(String.valueOf(orderId*numberOfItemsPerOrder+i));
			items.add(item);
		}
		return items;
	}

	private static List<Item> createItemNodes(List<Item> items) {
		for (Item item : items) {
            neo4jTemplate.save(item);
		}
		return items;
	}

	private static void relateOrderToItems(Order order, List<Item> items) {
        final Node orderNode = neo4jTemplate.getPersistentState(order);
        for (Item item : items) {
            final Node itemNode = neo4jTemplate.getPersistentState(item);
            neo4jTemplate.createRelationshipBetween(orderNode, itemNode, CONTAINS,null);
		}
	}

	private static long now() {
		return System.currentTimeMillis();
	}
}
