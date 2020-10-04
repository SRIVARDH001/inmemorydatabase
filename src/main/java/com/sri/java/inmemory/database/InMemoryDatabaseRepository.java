package com.sri.java.inmemory.database;

public interface InMemoryDatabaseRepository {
	void put(String key, String value);
	void put(String key, String value, String transactionId);
	String get(String key);
	String get(String key, String transactionId);
	void delete(String key);
	void delete(String key, String transactionId);
	void createTransaction(String transactionId);
	void rollbackTransaction(String transactionId);
	void commitTransaction(String transactionId);
}
