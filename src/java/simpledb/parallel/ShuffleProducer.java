package simpledb.parallel;

import java.util.ArrayList;

import org.apache.mina.core.future.IoFutureListener;
import org.apache.mina.core.future.WriteFuture;
import org.apache.mina.core.session.IoSession;

import simpledb.DbException;
import simpledb.DbIterator;
import simpledb.TransactionAbortedException;
import simpledb.Tuple;
import simpledb.TupleDesc;

/**
 * The producer part of the Shuffle Exchange operator.
 * 
 * ShuffleProducer distributes tuples to the workers according to some
 * partition function (provided as a PartitionFunction object during the
 * ShuffleProducer's instantiation).
 * 
 * */
public class ShuffleProducer extends Producer {

    private static final long serialVersionUID = 1L;
    
    private transient WorkingThread runningThread;

    private DbIterator child;
    private ParallelOperatorID operatorID;
    private SocketInfo[] workers;
    private PartitionFunction<?, ?> pf;
    
    public String getName() {
        return "shuffle_p";
    }

    public ShuffleProducer(DbIterator child, ParallelOperatorID operatorID,
            SocketInfo[] workers, PartitionFunction<?, ?> pf) {
        super(operatorID);
        this.child = child;
        this.operatorID = operatorID;
        this.workers = workers;
        this.pf = pf;
    }

    public void setPartitionFunction(PartitionFunction<?, ?> pf) {
        this.pf = pf;
    }

    public SocketInfo[] getWorkers() {
        return this.workers;
    }

    public PartitionFunction<?, ?> getPartitionFunction() {
        return this.pf;
    }

    class WorkingThread extends Thread {
        public void run() {

        	for (SocketInfo si : ShuffleProducer.this.workers) {
        		
        		IoSession session = ParallelUtility.createSession(si.getAddress(), ShuffleProducer.this.getThisWorker().minaHandler, -1);

                try {
                    ArrayList<Tuple> buffer = new ArrayList<Tuple>();
                    long lastTime = System.currentTimeMillis();

                    while (ShuffleProducer.this.child.hasNext()) {
                        Tuple tup = ShuffleProducer.this.child.next();
                        buffer.add(tup);
                        int cnt = buffer.size();
                        if (cnt >= TupleBag.MAX_SIZE) {
                            session.write(new TupleBag(
                            		ShuffleProducer.this.operatorID,
                            		ShuffleProducer.this.getThisWorker().workerID,
                                    buffer.toArray(new Tuple[] {}),
                                    ShuffleProducer.this.getTupleDesc()));
                            buffer.clear();
                            lastTime = System.currentTimeMillis();
                        }
                        if (cnt >= TupleBag.MIN_SIZE) {
                            long thisTime = System.currentTimeMillis();
                            if (thisTime - lastTime > TupleBag.MAX_MS) {
                                session.write(new TupleBag(
                                		ShuffleProducer.this.operatorID,
                                		ShuffleProducer.this.getThisWorker().workerID,
                                        buffer.toArray(new Tuple[] {}),
                                        ShuffleProducer.this.getTupleDesc()));
                                buffer.clear();
                                lastTime = thisTime;
                            }
                        }
                    }
                    if (buffer.size() > 0)
                        session.write(new TupleBag(ShuffleProducer.this.operatorID,
                        		ShuffleProducer.this.getThisWorker().workerID,
                                buffer.toArray(new Tuple[] {}),
                                ShuffleProducer.this.getTupleDesc()));
                    session.write(new TupleBag(ShuffleProducer.this.operatorID,
                    		ShuffleProducer.this.getThisWorker().workerID)).addListener(new IoFutureListener<WriteFuture>(){

                                @Override
                                public void operationComplete(WriteFuture future) {
                                    ParallelUtility.closeSession(future.getSession());
                                }});//.awaitUninterruptibly(); //wait until all the data have successfully transfered
                } catch (DbException e) {
                    e.printStackTrace();
                } catch (TransactionAbortedException e) {
                    e.printStackTrace();
                }
        	}
            
        }
    }

    @Override
    public void open() throws DbException, TransactionAbortedException {
        this.child.open();
        this.runningThread = new WorkingThread();
        this.runningThread.start();
        super.open();
    }

    public void close() {
        super.close();
        child.close();
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        throw new UnsupportedOperationException();
    }

    @Override
    public TupleDesc getTupleDesc() {
    	return this.child.getTupleDesc();
    }

    @Override
    protected Tuple fetchNext() throws DbException, TransactionAbortedException {
        try {
            runningThread.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return null;
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
