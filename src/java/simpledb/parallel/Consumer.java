package simpledb.parallel;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public abstract class Consumer extends Exchange {

    private static final long serialVersionUID = 1L;
    /**
     * The buffer for receiving ExchangeMessages. This buffer should be assigned
     * by the Worker. Basically,
     * buffer = Worker.inBuffer.get(this.getOperatorID())
     * */
    private transient volatile LinkedBlockingQueue<ExchangeMessage> buffer;

    public Consumer(ParallelOperatorID oID) {
        super(oID);
    }

    /**
     * Read a single ExchangeMessage from the queue that buffers incoming ExchangeMessages.
     * 
     * @param timeout
     *            Wait for at most timeout milliseconds. If the timeout is
     *            negative, wait until an element arrives.
     * */
    public ExchangeMessage take(int timeout) throws InterruptedException {

        if (timeout >= 0)
            return buffer.poll(timeout, TimeUnit.MILLISECONDS);
        else
            return buffer.take();
    }

    public void setBuffer(LinkedBlockingQueue<ExchangeMessage> buffer) {
        this.buffer = buffer;
    }

}
