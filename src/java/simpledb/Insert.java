package simpledb;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableid specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    private TransactionId tid;
    private DbIterator child;
    private int tableid;
    private int insertCount;
    
    /**
     * Constructor.
     * 
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableid
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t, DbIterator child, int tableid)
            throws DbException {
        this.tid = t;
        this.child = child;
        this.tableid = tableid;
        this.insertCount = -1;
    }

    public TupleDesc getTupleDesc() {
    	Type[] types = { Type.INT_TYPE };
    	return new TupleDesc(types);
    }

    public void open() throws DbException, TransactionAbortedException {
        this.child.open();
        updateInsertCount();
        super.open();
    }
    
    private void updateInsertCount() throws DbException, TransactionAbortedException {
        int iCount = 0;
        while (this.child.hasNext()) {
        	try {
				Database.getBufferPool().insertTuple(this.tid, this.tableid, child.next());
				iCount++;
			} catch (IOException e) {
				e.printStackTrace();
			}
        }
        this.insertCount = iCount;
    }

    public void close() {
        super.close();
        this.child.close();
        this.insertCount = -1;
    }

    public void rewind() throws DbException, TransactionAbortedException {
    	this.child.rewind();
    	updateInsertCount();
    }

    /**
     * Inserts tuples read from child into the tableid specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     * 
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
    	if (this.insertCount != -1) {
    		Tuple countTupleResult = getCountTuple();
    		this.insertCount = -1;
    		return countTupleResult;
    	} else {
    		return null;
    	}
    }
    
    private Tuple getCountTuple() {
        Type[] types = { Type.INT_TYPE };
        TupleDesc insertedCountTupleDesc = new TupleDesc(types);
        Tuple insertedCountTuple = new Tuple(insertedCountTupleDesc);
        insertedCountTuple.setField(0, new IntField(this.insertCount));
        return insertedCountTuple;
    }

    @Override
    public DbIterator[] getChildren() {
    	return new DbIterator[] { this.child };
    }

    @Override
    public void setChildren(DbIterator[] children) {
    	if (this.child != children[0]) {
    	    this.child = children[0];
    	}
    }
}