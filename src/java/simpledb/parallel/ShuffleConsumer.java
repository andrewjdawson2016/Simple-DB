package simpledb.parallel;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Iterator;
import simpledb.DbException;
import simpledb.DbIterator;
import simpledb.TransactionAbortedException;
import simpledb.Tuple;
import simpledb.TupleDesc;

/**
 * The consumer part of the Shuffle Exchange operator.
 * 
 * A ShuffleProducer operator sends tuples to all the workers according to some
 * PartitionFunction, while the ShuffleConsumer (this class) encapsulates the
 * methods to collect the tuples received at the worker from multiple source
 * workers' ShuffleProducer.
 * 
 * */
public class ShuffleConsumer extends Consumer {
    private static final long serialVersionUID = 1L;

    private transient Iterator<Tuple> tuples;
    
    private transient int innerBufferIndex;
    private transient ArrayList<TupleBag> innerBuffer;

    private TupleDesc td;
    private final BitSet workerEOS;
    private final SocketInfo[] sourceWorkers;
    private final HashMap<String, Integer> workerIdToIndex;
    
    private ShuffleProducer child;
    
    public String getName() {
        return "shuffle_c";
    }

    public ShuffleConsumer(ParallelOperatorID operatorID, SocketInfo[] workers) {
        this(null, operatorID, workers);
    }

    public ShuffleConsumer(ShuffleProducer child,
            ParallelOperatorID operatorID, SocketInfo[] workers) {
        super(operatorID);
        this.child = child;
        this.sourceWorkers = workers;
        this.workerIdToIndex = new HashMap<String, Integer>();
        int idx = 0;
        for (SocketInfo w : workers) {
            this.workerIdToIndex.put(w.getId(), idx++);
        }
        this.workerEOS = new BitSet(workers.length);
    }

    @Override
    public void open() throws DbException, TransactionAbortedException {
		this.tuples = null;
		this.innerBuffer = new ArrayList<TupleBag>();
		this.innerBufferIndex = 0;
		if (this.child != null)
		    this.child.open();
		super.open();
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
		this.tuples = null;
		this.innerBufferIndex = 0;
    }

    @Override
    public void close() {
		super.close();
		this.setBuffer(null);
		this.tuples = null;
		this.innerBufferIndex = -1;
		this.innerBuffer = null;
		this.workerEOS.clear();
    }

    @Override
    public TupleDesc getTupleDesc() {
        if (this.child != null)
            return this.child.getTupleDesc();
        else
            return this.td;

    }

    /**
     * 
     * Retrieve a batch of tuples from the buffer of ExchangeMessages. Wait if
     * the buffer is empty.
     * 
     * @return Iterator over the new tuples received from the source workers.
     *         Return <code>null</code> if all source workers have sent an end
     *         of file message.
     */
    Iterator<Tuple> getTuples() throws InterruptedException {
		TupleBag tb = null;
		if (this.innerBufferIndex < this.innerBuffer.size())
		    return this.innerBuffer.get(this.innerBufferIndex++).iterator();
	
		while (this.workerEOS.nextClearBit(0) < this.sourceWorkers.length) {
		    tb = (TupleBag)this.take(-1);
		    if (tb.isEos()) {
			this.workerEOS.set(this.workerIdToIndex.get(tb.getWorkerID()));
		    } else {
			innerBuffer.add(tb);
			this.innerBufferIndex++;
			return tb.iterator();
		    }
		}
		return null;
    }

    @Override
    protected Tuple fetchNext() throws DbException, TransactionAbortedException {
		while (tuples == null || !tuples.hasNext()) {
		    try {
			this.tuples = getTuples();
		    } catch (InterruptedException e) {
		        e.printStackTrace();
			throw new DbException(e.getLocalizedMessage());
		    }
		    if (tuples == null) // finish
			return null;
		}
		return tuples.next();

    }

    @Override
    public DbIterator[] getChildren() {
    	return new DbIterator[] { this.child };
    }

    @Override
    public void setChildren(DbIterator[] children) {
    	if (this.child != children[0]) {
    	    this.child = (ShuffleProducer) children[0];
    	}
    }

}
