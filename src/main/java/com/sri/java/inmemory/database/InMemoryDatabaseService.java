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


}
