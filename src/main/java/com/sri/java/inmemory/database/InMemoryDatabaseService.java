package com.sri.java.inmemory.database;

public class InMemoryDatabaseService {

	private InMemoryDatabaseRepository repository = InMemoryDatabaseRepositoryImpl.getInstance();

	public void create(String key, String value) {
		repository.put(key,value);
	}

	public void createWithTransactionId(String key, String value, String transactionId) {
		repository.put(key,value, transactionId);
	}

	public String find(String key) {
		return repository.get(key);
	}

	public String findByKeyAndTransactionId(String key, String transactinId) {
		return repository.get(key, transactinId);
	}

	public void delete(String key) {
		repository.delete(key);
	}

	public void delete(String key, String transactionId) {
		repository.delete(key, transactionId);
	}

	public void createTransaction(String transactionId) {
		repository.createTransaction(transactionId);
	}

	public void commitTransaction(String transactionId) {
		repository.commitTransaction(transactionId);
	}

	public void rollbackTransaction(String transactionId) {
		repository.rollbackTransaction(transactionId);
	}

}
