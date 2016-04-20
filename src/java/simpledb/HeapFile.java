package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {
	
	/**
	 * The file that stores the on-disk backing store for this heap file
	 */
	private File file;
	
	/**
	 * The schema for the table that this file repersents
	 */
	private TupleDesc schema;
	
    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
    	this.file = f;
    	this.schema = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return this.file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return this.file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.schema;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        int byteOffSet = pid.pageNumber() * BufferPool.getPageSize();
        if (byteOffSet + BufferPool.getPageSize() > this.file.length()) {
        	throw new IllegalArgumentException();
        }
        byte[] data = new byte[BufferPool.getPageSize()];
        try {
        	RandomAccessFile randomAccessFile = new RandomAccessFile(this.file, "r");
        	randomAccessFile.seek(byteOffSet);
        	randomAccessFile.read(data);
        	randomAccessFile.close();
            HeapPageId heapPageId = (HeapPageId) pid;
            return new HeapPage(heapPageId, data);
        } catch (Exception e) {
        	e.printStackTrace();
        	return null;
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
    	int pageNumber = page.getId().pageNumber();
    	int maxPageNumber = this.numPages();
    	if (pageNumber > maxPageNumber || pageNumber < 0) {
    		throw new IllegalArgumentException("attempted to write page at "
    				+ "invalid position: " + pageNumber);
    	}
        int byteOffSet = pageNumber * BufferPool.getPageSize();
        byte[] pageData = page.getPageData();
        try {
        	RandomAccessFile randomAccessFile = new RandomAccessFile(this.file, "rw");
        	randomAccessFile.seek(byteOffSet);
        	randomAccessFile.write(pageData);
        	randomAccessFile.close();
        } catch (Exception e) {
        	e.printStackTrace();
        }
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int) (this.file.length() / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
    	if (!t.getTupleDesc().equals(schema)) {
    		throw new DbException("tuple desc do not match for inserted tuple");
    	}
    	int numberOfPages = this.numPages();
    	int tableid = getId();
    	ArrayList<Page> pagesEffected = new ArrayList<Page>();
    	for (int i = 0; i < numberOfPages; i++) {
    		PageId currPageId = new HeapPageId(tableid, i);
    		Page currPage = Database.getBufferPool().getPage(tid, currPageId, Permissions.READ_WRITE);
    		int emptySlotCount = ((HeapPage) currPage).getNumEmptySlots();
    		if (emptySlotCount > 0) {
    			((HeapPage) currPage).insertTuple(t);
    			pagesEffected.add(currPage);
    			return pagesEffected;
    		}
    	}
    	
    	// if we got here it means there are no pages with space in this file
    	// so we need to create a new page and append it to the file
    	byte[] emptyPageData = HeapPage.createEmptyPageData();
    	HeapPageId emptyHeapPageId = new HeapPageId(tableid, numberOfPages);
    	Page emptyPage = new HeapPage(emptyHeapPageId, emptyPageData);
		((HeapPage) emptyPage).insertTuple(t);
    	this.writePage(emptyPage);
		pagesEffected.add(emptyPage);
		return pagesEffected;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
    	Page containingHeapPage = Database.getBufferPool().getPage(tid, 
    			t.getRecordId().getPageId(), Permissions.READ_WRITE);
    	((HeapPage) containingHeapPage).deleteTuple(t);
    	ArrayList<Page> pagesEffected = new ArrayList<Page>();
    	pagesEffected.add(containingHeapPage);
    	return pagesEffected;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        return new DbHeapFileIterator(this.getId(), this.numPages() - 1, tid);
    }
}