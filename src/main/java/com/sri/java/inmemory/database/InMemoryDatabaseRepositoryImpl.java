package com.sri.java.inmemory.database;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Assumptions made:
 * 1. Only one transaction can be in progress at a time. For example the below is not possible.
 * myDb.createTransaction(“abc”);
 * myDb.put(“a”, “foo”, “abc”);
 * myDb.createTransaction(“xyz”); // throws an error as transaction "abc" in in progress and has not been committed/rolledback.
 * 2. Even when a transaction is in progress, a modify request without transaction id is considered independent of the current
 * transaction (is auto commited and does not wait to be committed or rolled back).
 * 3. Keys are treated as primary key of a database. They are all UNIQUE.
 * 4. Transaction Ids are always UNIQUE.
 */
public class InMemoryDatabaseRepositoryImpl implements InMemoryDatabaseRepository {

	private static InMemoryDatabaseRepositoryImpl databaseRepository;

	private Map<String, DatabaseEntity> databaseMap;
	private Map<String, DatabaseEntity> currentPutTransactionMap;
	private Map<String, DatabaseEntity> currentDeleteTransactionMap;
	private List<String> previousTransactionId;
	private String currentTransactionId;
	private boolean isTransactionInProcess = false;

	private InMemoryDatabaseRepositoryImpl() {

	}

	static InMemoryDatabaseRepositoryImpl getInstance() {
		if (databaseRepository == null) {
			databaseRepository = new InMemoryDatabaseRepositoryImpl();
			databaseRepository.databaseMap = new HashMap<String, DatabaseEntity>();
			databaseRepository.currentPutTransactionMap = new HashMap<String, DatabaseEntity>();
			databaseRepository.currentDeleteTransactionMap = new HashMap<String, DatabaseEntity>();
			databaseRepository.previousTransactionId = new ArrayList<String>();
		}
		return databaseRepository;
	}

	public void put(String key, String value) {
		put(key, value, null);
	}

	public void put(String key, String value, String transactionId) {
		if (key == null) {
			throw new InMemoryDatabaseException("Invalid input! key cannot be null");
		}
		DatabaseEntity databaseEntity = new DatabaseEntity();
		databaseEntity.setKey(key);
		databaseEntity.setValue(value);
		databaseEntity.setTransactionId(transactionId);
		if (transactionId != null) {
			checkIfTransactionDetailsAreCorrect(transactionId);
			currentPutTransactionMap.put(key, databaseEntity);
		} else {
			databaseMap.put(key, databaseEntity);
		}
	}

	public String get(String key) {
		if ((currentPutTransactionMap == null || currentPutTransactionMap.size() ==0) && (databaseMap == null || databaseMap.size() == 0)) {
			throw new InMemoryDatabaseException("Error!The in memory database is empty");
		} else if (isTransactionInProcess) {
			System.out.println("Another transaction in progress and cannot find value for key " + key);
			if (databaseMap.containsKey(key)) { // first search committed transaction.
				return databaseMap.get(key).getValue();
			} else if (currentPutTransactionMap.containsKey(key)
					&& currentPutTransactionMap.get(key).getTransactionId().equals(currentTransactionId)) {
				//return currentPutTransactionMap.get(key).getValue();
				return null;
			}
		}
		else {
			return databaseMap.get(key).getValue();
		}
		return null;
	}

	public String get(String key, String transactionId) {
		if (isTransactionInProcess && transactionId.equals(currentTransactionId)) {
			if (currentPutTransactionMap.containsKey(key)) {
				return currentPutTransactionMap.get(key).getValue();
			}
		} else {
			DatabaseEntity entity = databaseMap.get(key);
			if (databaseMap.containsKey(key)) {
				return entity.getValue();
			} else {
				System.out.println("Cannot find value fo key:  " + key);
				return null;
			}
		}
		return null;
	}

	public void delete(String key) {
		if (databaseMap == null || databaseMap.size() == 0) {
			throw new InMemoryDatabaseException("Error!The in memory database is empty");
		}
		if (!databaseMap.containsKey(key)) {
			throw new InMemoryDatabaseException("Error!The key " + key + " is not present in DB");
		}
		databaseMap.remove(key);
	}

	public void delete(String key, String transactionId) {
		checkIfTransactionDetailsAreCorrect(transactionId);
		DatabaseEntity databaseEntity = new DatabaseEntity();
		databaseEntity.setKey(key);
		databaseEntity.setTransactionId(transactionId);
		if (transactionId != null) {
			currentDeleteTransactionMap.put(key, databaseEntity);
		}
	}

	public void createTransaction(String transactionId) {
		if (previousTransactionId.contains(transactionId)) {
			throw new InMemoryDatabaseException("Error!Cannot create a transaction - another transaction with same id "
					+transactionId+" exists");
		}
		if (isTransactionInProcess && currentTransactionId != null) {
			System.out.println("Another transaction in progress and hence clearing those details");
			previousTransactionId.add(currentTransactionId);
			currentTransactionId = transactionId;
			isTransactionInProcess = true;
			clearUncommittedTransactions();
		}  else {
			currentTransactionId = transactionId;
			isTransactionInProcess = true;
		}
	}

	public void rollbackTransaction(String transactionId) {
		checkIfTransactionDetailsAreCorrect(transactionId);
		if (currentPutTransactionMap != null && currentPutTransactionMap.size() > 0) {
			currentPutTransactionMap.clear();
		}
		if (currentDeleteTransactionMap != null && currentDeleteTransactionMap.size() > 0) {
			currentDeleteTransactionMap.clear();
		}
		currentTransactionId = null;
		isTransactionInProcess = false;
		previousTransactionId.add(transactionId);
	}

	public void commitTransaction(String transactionId) {
		if (previousTransactionId.contains(transactionId)) {
			throw new InMemoryDatabaseException("Error!Cannot commit previous transaction id - obsolete now");
		}
		if (currentTransactionId != null && !currentTransactionId.equals(transactionId)) {
			throw new InMemoryDatabaseException("Error!Not the current transaction in process");
		}
		if (currentPutTransactionMap.size() > 0) {
			for (Map.Entry<String, DatabaseEntity> entry : currentPutTransactionMap.entrySet()) {
				databaseMap.put(entry.getKey(), entry.getValue());
			}
		}
		if (currentDeleteTransactionMap.size() > 0) {
			for (Map.Entry<String, DatabaseEntity> entry : currentDeleteTransactionMap.entrySet()) {
				DatabaseEntity entity = databaseMap.get(entry.getKey());
				if (entity.getTransactionId().equals(transactionId)) {
					databaseMap.remove(entry.getKey());
				}
			}

		}
		currentTransactionId = null;
		isTransactionInProcess = false;
	}

	private void checkIfTransactionDetailsAreCorrect(String transactionId) {
		if (!isTransactionInProcess) {
			throw new InMemoryDatabaseException("Error!No transaction in progress");
		} else if (!currentTransactionId.equals(transactionId)) {
			throw new InMemoryDatabaseException("Error!Not the current transaction in process");
		} else if (previousTransactionId.contains(transactionId)) {
			throw new InMemoryDatabaseException("Error!Transaction id invalid - already was part of a rolled back transaction");
		}
	}

	private void clearUncommittedTransactions() {
		currentPutTransactionMap.clear();
		currentDeleteTransactionMap.clear();
	}

}
