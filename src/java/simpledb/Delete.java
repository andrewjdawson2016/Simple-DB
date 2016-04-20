package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

    private TransactionId tid;
    private DbIterator child;
    
    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, DbIterator child) {
        this.tid = t;
        this.child = child;
    }

    public TupleDesc getTupleDesc() {
    	Type[] types = { Type.INT_TYPE };
    	return new TupleDesc(types);
    }

    public void open() throws DbException, TransactionAbortedException {
        this.child.open();
        super.open();
    }

    public void close() {
        super.close();
        this.child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
    	this.child.rewind();
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if (!child.hasNext()) {
        	return null;
        }
        
        int deletedCount = 0;
        while (child.hasNext()) {
        	try {
				Database.getBufferPool().deleteTuple(this.tid, child.next());
				deletedCount++;
			} catch (IOException e) {
				e.printStackTrace();
				return null;
			}
        }
        return getCountTuple(deletedCount);
    }
    
    private Tuple getCountTuple(int deletedCount) {
        Type[] types = { Type.INT_TYPE };
        TupleDesc deletedCountTupleDesc = new TupleDesc(types);
        Tuple deletedCountTuple = new Tuple(deletedCountTupleDesc);
        deletedCountTuple.setField(0, new IntField(deletedCount));
        return deletedCountTuple;
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
