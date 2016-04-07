package simpledb;

import java.util.Arrays;
import java.util.Iterator;

public class DbHeapFileIterator extends AbstractDbFileIterator {

	/**
	 * The current page number we are on
	 */
	private int currentPageNumber;
	
	/**
	 * The tableid of the table this iterator is going over
	 */
	private int tableid;
	
	/**
	 * The iterator over the current page
	 */
	private Iterator<Tuple> currentIterator;
	
	/**
	 * The highest valid page number
	 */
	private int highestPageNumber;
	
	/**
	 * Constructs a new DbHeapFileIterator
	 */
	public DbHeapFileIterator(int tableid, int highestPageNumber) {
		this.tableid = tableid;
		this.currentPageNumber = 0;
		this.currentIterator = null;
		this.highestPageNumber = highestPageNumber;
	}
	
	@Override
	public void open() throws DbException, TransactionAbortedException {
		HeapPageId pid = new HeapPageId(this.tableid, this.currentPageNumber);
		HeapPage page = (HeapPage) Database.getBufferPool().getPage(new TransactionId(), 
				pid, Permissions.READ_ONLY);
		this.currentIterator = page.iterator();
	}

	@Override
	public void rewind() throws DbException, TransactionAbortedException {
		open();
	}

	@Override
	protected Tuple readNext() throws DbException, TransactionAbortedException {
		if (this.currentIterator == null) {
			return null;
		} else if (this.currentIterator.hasNext()) {
			return this.currentIterator.next();
		} else if (this.currentPageNumber == this.highestPageNumber) {
			return null;
		} else {
			this.currentPageNumber++;
			open();
			return this.currentIterator.next();
		}
	}
	
	@Override
    public void close() {
        super.close();
        this.currentIterator = null;
    }
}