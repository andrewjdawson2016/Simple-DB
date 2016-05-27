package simpledb;

import java.util.Iterator;
import java.io.Serializable;

public class DbHeapFileIterator extends AbstractDbFileIterator implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	/**
	 * The current page number we are on
	 */
	private int currentPageNumber;
	
	/**
	 * The tableid of the table this iterator is going over.
	 */
	private int tableid;
	
	/**
	 * The iterator over the current page
	 */
	private Iterator<Tuple> currentIterator;
	
	/**
	 * The TransactionId for this iterator
	 */
	private TransactionId tid;
	
	/**
	 * The highest valid page number
	 */
	private int highestPageNumber;
	
	/**
	 * Constructs a new DbHeapFileIterator
	 */
	public DbHeapFileIterator(int tableid, int highestPageNumber, TransactionId tid) {
		this.tableid = tableid;
		this.currentIterator = null;
		this.highestPageNumber = highestPageNumber;
		this.tid = tid;
	}
	
	@Override
	public void open() throws DbException, TransactionAbortedException {
		this.currentPageNumber = 0;
		updateIterator();
	}

	@Override
	public void rewind() throws DbException, TransactionAbortedException {
		close();
		open();
	}

	@Override
	protected Tuple readNext() throws DbException, TransactionAbortedException {
		if (this.currentIterator == null) {
			return null;
		} 
		
		else if (this.currentIterator.hasNext()) {
			return this.currentIterator.next();
		} 
		
		else if (this.currentPageNumber == this.highestPageNumber) {
			this.currentIterator = null;
			return null;
		} 
		
		else {
			this.currentPageNumber = this.currentPageNumber + 1;
			updateIterator();
			if (this.currentIterator.hasNext()) {
				return this.currentIterator.next();
			} else {
				return null;
			}
		}
	}
	
	private void updateIterator() throws TransactionAbortedException, DbException {
		HeapPageId pid = new HeapPageId(this.tableid, this.currentPageNumber);
		HeapPage page = (HeapPage) Database.getBufferPool().getPage(this.tid, 
				pid, Permissions.READ_ONLY);
		this.currentIterator = page.iterator();
	}
		
	@Override
    public void close() {
        super.close();
        this.currentIterator = null;
    }
}