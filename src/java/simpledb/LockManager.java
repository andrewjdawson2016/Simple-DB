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
		if (canAcquire(tid, pid, perm)) {
			if (this.transactionLocks.containsKey(tid)) {
				this.transactionLocks.get(tid).add(pid);
			}
			
			if (perm == Permissions.READ_ONLY) {
				if (this.readLocks.containsKey(pid)) {
					int readLockCount = this.readLocks.get(pid);
					this.readLocks.put(pid, readLockCount + 1);
				} else {
					this.readLocks.put(pid, 1);
				}
			} else {
				this.writeLocks.add(pid);
			}
		}
	}
	
	/**
	 * Releases lock with given pid on behalf of given transactionId.
	 * 
	 * @param tid transactionId of the transaction requesting the release
	 * @param pid the page over which the lock is being released
	 */
	public synchronized void releaseLock(TransactionId tid, PageId pid) {
		if (this.transactionLocks.containsKey(tid) && this.transactionLocks.get(tid).contains(pid)) {
			this.transactionLocks.get(tid).remove(pid);
			if (this.transactionLocks.get(tid).isEmpty()) {
				this.transactionLocks.remove(tid);
			}
			removeLock(pid);
		}
	}
	
	/**
	 * Release all locks held by transaction with given tid.
	 * 
	 * @param tid the transactionId of the transaction requesting the full lock release
	 */
	public synchronized void releaseAllLocks(TransactionId tid) {
		if (this.transactionLocks.containsKey(tid)) {
			for (PageId pid : this.transactionLocks.get(tid)) {
				removeLock(pid);
			}
			this.transactionLocks.remove(tid);
		}
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
		return this.transactionLocks.containsKey(tid) && this.transactionLocks.get(tid).contains(pid);
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
		if (this.transactionLocks.containsKey(tid) && this.transactionLocks.get(tid).contains(pid)) {
			if (perm == Permissions.READ_ONLY) {
				return this.readLocks.containsKey(pid);
			}
			
			if (perm == Permissions.READ_WRITE) {
				return this.writeLocks.contains(pid);
			}
		}
		
		return false;
	}
	
	/**
	 * Returns true if the lock with pid is available, to be acquired by tid with given permissions
	 * Returns false if tid already has lock on pid
	 * 
	 * @param tid the transactionId of the transaction checking availability
	 * @param pid the page being checked for availability
	 * @param perm the permission level for which the request is checking
	 * 
	 * @return true if the lock with pid is available, to be acquired by tid with given permissions
	 * Returns false if tid already has lock on pid
	 */
	public synchronized boolean canAcquire(TransactionId tid, PageId pid, Permissions perm) {
		if (this.hasLock(tid, pid, perm)) {
			return false;
		}
		
		boolean pageNotLocked = (!this.readLocks.containsKey(pid)) && (!this.writeLocks.contains(pid));
		boolean onlyReads = (!this.writeLocks.contains(pid)) && (perm == Permissions.READ_ONLY);
		if (pageNotLocked || onlyReads) {
			return true;
		}
		
		if (!this.writeLocks.contains(pid) && this.readLocks.containsKey(pid) && 
				this.readLocks.get(pid) == 1 && this.transactionLocks.get(tid).contains(pid) &&
				perm == Permissions.READ_WRITE) {
			return true;
		}
		
		return false;
	}
	
	// Helper method to release locks, removes readLocks or writeLocks without affecting this.transactionLocks
	private synchronized void removeLock(PageId pid) {
		if (this.readLocks.containsKey(pid)) {
			int readLockCount = this.readLocks.get(pid);
			if (readLockCount < 2) {
				this.readLocks.remove(pid);
			} else {
				this.readLocks.put(pid, readLockCount - 1);
			}
		} else {
			this.writeLocks.remove(pid);
		}
	}
}