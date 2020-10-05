package com.sri.java.inmemory.database.service;

import com.sri.java.inmemory.database.InMemoryDatabaseException;
import com.sri.java.inmemory.database.InMemoryDatabaseService;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class InMemoryDatabaseServiceTest {

	private InMemoryDatabaseService service;

	@Before
	public void init() {
		service = new InMemoryDatabaseService();
	}

	@Test
	public void testHappySequence() {
		service.createTransaction("abc");
		service.createWithTransactionId("a","foo","abc");
		String foo = service.findByKeyAndTransactionId("a","abc");
		Assert.assertEquals("foo", foo);
		String what = service.find("a");
		Assert.assertEquals(null, what);

		service.createTransaction("xyz");
		service.createWithTransactionId("a","bar","xyz");
		String bar = service.findByKeyAndTransactionId("a","xyz");
		Assert.assertEquals("bar", bar);
		service.commitTransaction("xyz");

		bar = service.find("a");
		Assert.assertEquals("bar", bar);
	}

	@Test(expected = InMemoryDatabaseException.class)
	public void testFailureSequence() {
		service.createTransaction("abc");
		service.createWithTransactionId("a","foo","abc");
		String foo = service.findByKeyAndTransactionId("a","abc");
		Assert.assertEquals("foo", foo);
		String what = service.find("a");
		Assert.assertEquals(null, what);

		service.createTransaction("xyz");
		service.createWithTransactionId("a","bar","xyz");
		String bar = service.findByKeyAndTransactionId("a","xyz");
		Assert.assertEquals("bar", bar);
		service.commitTransaction("xyz");

		bar = service.find("a");
		Assert.assertEquals("bar", bar);

		service.commitTransaction("abc");
	}

	@Test(expected = InMemoryDatabaseException.class)
	public void testAnotherFailureSequence() {
		service.createTransaction("abc");
		service.createWithTransactionId("a","foo","abc");
		String foo = service.findByKeyAndTransactionId("a","abc");
		Assert.assertEquals("foo", foo);
		String what = service.find("a");
		Assert.assertEquals(null, what);

		service.createTransaction("xyz");
		service.createWithTransactionId("a","bar","xyz");
		String bar = service.findByKeyAndTransactionId("a","xyz");
		Assert.assertEquals("bar", bar);
		service.commitTransaction("xyz");

		bar = service.find("a");
		Assert.assertEquals("bar", bar);

		service.createTransaction("pqr");
		service.createWithTransactionId("a","foo","pqr");
		service.rollbackTransaction("pqr");
		service.createWithTransactionId("a","foo","pqr");

	}
}
