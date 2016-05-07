package simpledb;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class LockManager {
	public Map<TransactionId, Set<PageId>> transactionLocks;
	public Map<PageId, Set<TransactionId>> readLocks;
	public HashMap<PageId, TransactionId> writeLocks;
	
	public LockManager() {
		this.transactionLocks = new HashMap<TransactionId, Set<PageId>>();
		this.readLocks = new HashMap<PageId, Set<TransactionId>>();
		this.writeLocks = new HashMap<PageId, TransactionId>();
	}
	
	/**
	 * Acquires a lock on behalf of given transaction with given permission.
	 * 
	 * @param tid transactionId of the transaction requesting the lock
	 * @param pid the page over which the lock is being requested
	 * @param perm the permission level the transaction is requesting
	 * @throws InterruptedException 
	 * @throws TransactionAbortedException if deadlock was detected
	 */
	public synchronized void acquireLock(TransactionId tid, PageId pid, Permissions perm) 
			throws TransactionAbortedException {

		if (!this.hasLock(tid, pid, perm)) {
			long startTime = System.currentTimeMillis();
			while (!this.canAcquire(tid, pid, perm)) {
				try {
					wait(50);
				} catch (InterruptedException e) {
					throw new TransactionAbortedException();
				}
				long totalWaitTime = System.currentTimeMillis() - startTime;
				if (totalWaitTime > 2500) {
					throw new TransactionAbortedException();
				}
			}
			
			// here allowed to acquire lock
			if (!this.transactionLocks.containsKey(tid)) {
				this.transactionLocks.put(tid, new HashSet<PageId>());
			}
			this.transactionLocks.get(tid).add(pid);
			
			if (perm == Permissions.READ_ONLY) {
				if (!this.readLocks.containsKey(pid)) {
					this.readLocks.put(pid, new HashSet<TransactionId>());
				}
				this.readLocks.get(pid).add(tid);
			} else {
				this.writeLocks.put(pid, tid);
			}
		}
		notifyAll();
	}
	
	/**
	 * Releases lock with given pid on behalf of given transactionId.
	 * 
	 * @param tid transactionId of the transaction requesting the release
	 * @param pid the page over which the lock is being released
	 */
	public synchronized void releaseLock(TransactionId tid, PageId pid) {
		if (hasLock(tid, pid)) {
			
			this.transactionLocks.get(tid).remove(pid);
			if (this.transactionLocks.get(tid).isEmpty()) {
				this.transactionLocks.remove(tid);
			}
			
			this.removeLock(pid, tid);
		}
		notifyAll();
	}
	
	/**
	 * Release all locks held by transaction with given tid.
	 * 
	 * @param tid the transactionId of the transaction requesting the full lock release
	 */
	public synchronized void releaseAllLocks(TransactionId tid) {
		if (this.transactionLocks.containsKey(tid)) {
			for (PageId pid : this.transactionLocks.get(tid)) {
				this.removeLock(pid, tid);
			}
			this.transactionLocks.remove(tid);
		}
		notifyAll();
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
		if (perm == Permissions.READ_ONLY) {
			return (this.readLocks.containsKey(pid) && this.readLocks.get(pid).contains(tid))
					|| (this.writeLocks.containsKey(pid) && this.writeLocks.get(pid).equals(tid));
		} else {
			return this.writeLocks.containsKey(pid) && this.writeLocks.get(pid).equals(tid);
		}
	}
	
	/**
	 * Returns true if the lock with pid is available, to be acquired by tid with given permissions
	 * Returns true if tid already holds the given lock
	 * 
	 * @param tid the transactionId of the transaction checking availability
	 * @param pid the page being checked for availability
	 * @param perm the permission level for which the request is checking
	 * 
	 * @return true if the lock with pid is available, to be acquired by tid with given permissions
	 * Returns true if tid already holds the given lock
	 */
	public synchronized boolean canAcquire(TransactionId tid, PageId pid, Permissions perm) {
		if (this.hasLock(tid, pid, perm)) {
			return true;
		}
		
		boolean pageNotLocked = (!this.readLocks.containsKey(pid)) && (!this.writeLocks.containsKey(pid));
		boolean onlyReads = (!this.writeLocks.containsKey(pid)) && (perm == Permissions.READ_ONLY);
		if (pageNotLocked || onlyReads) {
			return true;
		}
		
		if ((perm == Permissions.READ_WRITE) && this.canUpgrade(tid, pid)) {
			return true;
		}
		
		return false;
	}
	
	// Helper method to check if a tid can be upgraded
	private synchronized boolean canUpgrade(TransactionId tid, PageId pid) {
		return this.transactionLocks.containsKey(tid)
				&& this.transactionLocks.get(tid).contains(pid)
				&& this.readLocks.containsKey(pid)
				&& this.readLocks.get(pid).contains(tid)
				&& (this.readLocks.get(pid).size() == 1)
				&& (!this.writeLocks.containsKey(pid));
	}
	
	// Helper method to release locks, removes readLocks or writeLocks without affecting this.transactionLocks
	private synchronized void removeLock(PageId pid, TransactionId tid) {
		if (this.readLocks.containsKey(pid) && this.readLocks.get(pid).contains(tid)) {
			this.readLocks.get(pid).remove(tid);
			if (this.readLocks.get(pid).isEmpty()) {
				this.readLocks.remove(pid);
			}
		}
		
		if (this.writeLocks.containsKey(pid) && this.writeLocks.get(pid).equals(tid)) {
			this.writeLocks.remove(pid);
		}
	}
	
	public synchronized int countTidRemoveLater(TransactionId tid) {
		if (this.transactionLocks.containsKey(tid)) {
			return this.transactionLocks.get(tid).size();
		} else {
			return 0;
		}
	}
}