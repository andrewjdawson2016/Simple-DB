package simpledb.parallel;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
        	Map<Integer, List<Tuple>> buffers = new HashMap<Integer, List<Tuple>>();
        	List<IoSession> sessions = new ArrayList<IoSession>();
        	
        	for (int i = 0; i < workers.length; i++) {
               sessions.add(ParallelUtility.createSession(
                        ShuffleProducer.this.workers[i].getAddress(),
                        ShuffleProducer.this.getThisWorker().minaHandler, -1));
               buffers.put(i, new ArrayList<Tuple>());
        	}
                

            try {
                long lastTime = System.currentTimeMillis();

                while (ShuffleProducer.this.child.hasNext()) {
                    Tuple tup = ShuffleProducer.this.child.next();
                    int parition = pf.partition(tup, ShuffleProducer.this.child.getTupleDesc());
                    List<Tuple> buffer = buffers.get(parition);
                    IoSession session = sessions.get(parition);
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
                
                for (int parition : buffers.keySet()) {
                	List<Tuple> buffer = buffers.get(parition);
                	IoSession session = sessions.get(parition);
	                if (buffer.size() > 0) {
	                    session.write(new TupleBag(ShuffleProducer.this.operatorID,
	                    		ShuffleProducer.this.getThisWorker().workerID,
	                            buffer.toArray(new Tuple[] {}),
	                            ShuffleProducer.this.getTupleDesc()));
	                    buffer.clear();
	                }
	                session.write(new TupleBag(ShuffleProducer.this.operatorID,
	                		ShuffleProducer.this.getThisWorker().workerID)).addListener(new IoFutureListener<WriteFuture>(){
	
	                            @Override
	                            public void operationComplete(WriteFuture future) {
	                                ParallelUtility.closeSession(future.getSession());
	                            }});
                }
                
            } catch (DbException e) {
                e.printStackTrace();
            } catch (TransactionAbortedException e) {
                e.printStackTrace();
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
