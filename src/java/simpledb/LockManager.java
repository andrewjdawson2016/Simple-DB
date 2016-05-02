package simpledb;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class LockManager {
	private Map<TransactionId, Set<PageId>> transactionLocks;
	private Map<PageId, Integer> readLocks;
	private Set<PageId> writeLocks;
	
	public LockManager() {
		this.transactionLocks = new HashMap<TransactionId, Set<PageId>>();
		this.readLocks = new HashMap<PageId, Integer>();
		this.writeLocks = new HashSet<PageId>();
	}
	
	/**
	 * Acquires a lock on behalf of given transaction with given permission.
	 * 
	 * @param tid transactionId of the transaction requesting the lock
	 * @param pid the page over which the lock is being requested
	 * @param perm the permission level the transaction is requesting
	 */
	public synchronized void acquireLock(TransactionId tid, PageId pid, Permissions perm) {
	}
	
	/**
	 * Releases lock with given pid on behalf of given transactionId.
	 * 
	 * @param tid transactionId of the transaction requesting the release
	 * @param pid the page over which the lock is being released
	 */
	public synchronized void releaseLock(TransactionId tid, PageId pid) {
	}
	
	/**
	 * Release all locks held by transaction with given tid.
	 * 
	 * @param tid the transactionId of the transaction requesting the full lock release
	 */
	public synchronized void releaseAllLocks(TransactionId tid) {
	}
	
	/**
	 * Returns true if tid has a read lock or a write lock on page with pid, false otherwise
	 * 
	 * @param tid transactionId of the transaction being checked for the lock with pid
	 * @param pid the page being checked for a lock
	 * 
	 * @return true if tid has a read lock or a write lock on page with pid, false otherwise
	 */
	public synchronized boolean hasLock(TransactionId tid, PageId pid) {
		return false;
	}
	
	/**
	 * Return true if tid has a lock on page with pid with given permissions, false otherwise
	 * 
	 * @param tid transactionId of the transaction being checked for the lock with pid
	 * @param pid the page being checked for a lock
	 * @param perm the level of permissions of the lock to check
	 * 
	 * @return true if tid has a lock on page with pid with given permissions, false otherwise
	 */
	public synchronized boolean hasLock(TransactionId tid, PageId pid, Permissions perm) {
		return false;
	}
}