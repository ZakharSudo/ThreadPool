import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.Condition;
import java.util.logging.*;

public class CustomThreadPool implements CustomExecutor {
    private static final Logger logger = Logger.getLogger(CustomThreadPool.class.getName());
    
    private final int corePoolSize;
    private final int maxPoolSize;
    private final long keepAliveTime;
    private final TimeUnit timeUnit;
    private final int queueSize;
    private final int minSpareThreads;
    private final RejectedExecutionHandler rejectedHandler;
    private final ThreadFactory threadFactory;
    
    private final List<Worker> workers = new ArrayList<>();
    private final List<BlockingQueue<Runnable>> taskQueues;
    private final AtomicInteger currentQueueIndex = new AtomicInteger(0);
    private final ReentrantLock mainLock = new ReentrantLock();
    private final Condition terminationCondition = mainLock.newCondition();
    
    private volatile boolean isShutdown = false;
    private final AtomicInteger activeThreads = new AtomicInteger(0);
    private final AtomicInteger idleThreads = new AtomicInteger(0);
    
    public enum RejectedPolicy {
        ABORT, CALLER_RUNS, DISCARD, DISCARD_OLDEST
    }
    
    public static class Builder {
        private int corePoolSize = 2;
        private int maxPoolSize = 4;
        private long keepAliveTime = 60;
        private TimeUnit timeUnit = TimeUnit.SECONDS;
        private int queueSize = 100;
        private int minSpareThreads = 1;
        private RejectedPolicy policy = RejectedPolicy.ABORT;
        
        public Builder corePoolSize(int size) { this.corePoolSize = size; return this; }
        public Builder maxPoolSize(int size) { this.maxPoolSize = size; return this; }
        public Builder keepAliveTime(long time, TimeUnit unit) { this.keepAliveTime = time; this.timeUnit = unit; return this; }
        public Builder queueSize(int size) { this.queueSize = size; return this; }
        public Builder minSpareThreads(int threads) { this.minSpareThreads = threads; return this; }
        public Builder rejectedPolicy(RejectedPolicy policy) { this.policy = policy; return this; }
        
        public CustomThreadPool build() {
            return new CustomThreadPool(this);
        }
    }
    
    private CustomThreadPool(Builder builder) {
        this.corePoolSize = builder.corePoolSize;
        this.maxPoolSize = builder.maxPoolSize;
        this.keepAliveTime = builder.keepAliveTime;
        this.timeUnit = builder.timeUnit;
        this.queueSize = builder.queueSize;
        this.minSpareThreads = builder.minSpareThreads;
        
        int numQueues = Math.max(1, corePoolSize);
        this.taskQueues = new ArrayList<>(numQueues);
        for (int i = 0; i < numQueues; i++) {
            taskQueues.add(new LinkedBlockingQueue<>(queueSize));
        }
        
        this.threadFactory = new CustomThreadFactory();
        
        this.rejectedHandler = (task, executor) -> {
            switch (builder.policy) {
                case ABORT:
                    throw new RejectedExecutionException("Task rejected: " + task);
                case CALLER_RUNS:
                    logger.info("[Rejected] Task executed in caller thread due to overload");
                    task.run();
                    break;
                case DISCARD:
                    logger.info("[Rejected] Task discarded: " + task);
                    break;
                case DISCARD_OLDEST:
                    BlockingQueue<Runnable> mostLoadedQueue = null;
                    int maxSize = 0;
                    for (BlockingQueue<Runnable> q : taskQueues) {
                        if (q.size() > maxSize) {
                            maxSize = q.size();
                            mostLoadedQueue = q;
                        }
                    }
                    if (mostLoadedQueue != null && !mostLoadedQueue.isEmpty()) {
                        mostLoadedQueue.poll();
                        logger.info("[Rejected] Discarded oldest task from most loaded queue");
                        if (!executor.tryAddTask(task)) {
                            logger.warning("[Rejected] Still cannot add task after discarding oldest");
                        }
                    }
                    break;
            }
        };
        
        for (int i = 0; i < corePoolSize; i++) {
            addWorker(false);
        }
        
        logger.info("[Pool] Initialized with core=" + corePoolSize + ", max=" + maxPoolSize + 
                   ", queueSize=" + queueSize + ", minSpare=" + minSpareThreads);
    }
    
    private boolean tryAddTask(Runnable task) {
        mainLock.lock();
        try {
            if (isShutdown) return false;
            
            int numQueues = taskQueues.size();
            int startIndex = currentQueueIndex.getAndIncrement() % numQueues;
            
            for (int i = 0; i < numQueues; i++) {
                int idx = (startIndex + i) % numQueues;
                BlockingQueue<Runnable> queue = taskQueues.get(idx);
                if (queue.remainingCapacity() > 0) {
                    queue.offer(task);
                    logger.info("[Pool] Task accepted into queue #" + idx + ": " + task);
                    
                    if (idleThreads.get() < minSpareThreads && activeThreads.get() < maxPoolSize) {
                        addWorker(true);
                    }
                    return true;
                }
            }
            
            if (activeThreads.get() < maxPoolSize) {
                addWorker(true);
                BlockingQueue<Runnable> firstQueue = taskQueues.get(0);
                if (firstQueue.remainingCapacity() > 0) {
                    firstQueue.offer(task);
                    logger.info("[Pool] Task accepted after creating new worker");
                    return true;
                }
            }
            
            return false;
        } finally {
            mainLock.unlock();
        }
    }
    
    private void addWorker(boolean isSpare) {
        mainLock.lock();
        try {
            if (isShutdown) return;
            if (activeThreads.get() >= maxPoolSize) return;
            
            Worker worker = new Worker();
            Thread thread = threadFactory.newThread(worker);
            workers.add(worker);
            activeThreads.incrementAndGet();
            worker.thread = thread;
            thread.start();
            logger.info("[Pool] Added new worker, active threads: " + activeThreads.get());
        } finally {
            mainLock.unlock();
        }
    }
    
    @Override
    public void execute(Runnable command) {
        if (command == null) throw new NullPointerException();
        if (isShutdown) {
            logger.warning("[Pool] Rejected task because pool is shutting down");
            throw new RejectedExecutionException("Pool is shutting down");
        }
        
        if (!tryAddTask(command)) {
            rejectedHandler.rejectedExecution(command, this);
        }
    }
    
    @Override
    public <T> Future<T> submit(Callable<T> callable) {
        FutureTask<T> future = new FutureTask<>(callable);
        execute(future);
        return future;
    }
    
    @Override
    public void shutdown() {
        mainLock.lock();
        try {
            if (isShutdown) return;
            isShutdown = true;
            logger.info("[Pool] Shutdown initiated");
            
            for (Worker w : workers) {
                if (w.thread != null && w.thread.isAlive()) {
                    w.thread.interrupt();
                }
            }
        } finally {
            mainLock.unlock();
        }
    }
    
    @Override
    public void shutdownNow() {
        shutdown();
        for (BlockingQueue<Runnable> queue : taskQueues) {
            List<Runnable> remaining = new ArrayList<>();
            queue.drainTo(remaining);
            logger.info("[Pool] Cleared " + remaining.size() + " pending tasks");
        }
    }
    
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        long nanos = unit.toNanos(timeout);
        mainLock.lock();
        try {
            while (activeThreads.get() > 0 && nanos > 0) {
                nanos = terminationCondition.awaitNanos(nanos);
            }
            return activeThreads.get() == 0;
        } finally {
            mainLock.unlock();
        }
    }
    
    private class Worker implements Runnable {
        private Thread thread;
        private volatile boolean running = true;
        
        @Override
        public void run() {
            int queueIndex = workers.indexOf(this) % taskQueues.size();
            BlockingQueue<Runnable> myQueue = taskQueues.get(queueIndex);
            
            while (running && !Thread.currentThread().isInterrupted()) {
                try {
                    idleThreads.incrementAndGet();
                    Runnable task = myQueue.poll(keepAliveTime, timeUnit);
                    idleThreads.decrementAndGet();
                    
                    if (task == null) {
                        mainLock.lock();
                        try {
                            if (activeThreads.get() > corePoolSize || 
                                (activeThreads.get() > minSpareThreads && isShutdown)) {
                                logger.info("[Worker] " + Thread.currentThread().getName() + 
                                          " idle timeout, stopping (active=" + activeThreads.get() + 
                                          ", core=" + corePoolSize + ")");
                                running = false;
                                break;
                            } else {
                                continue;
                            }
                        } finally {
                            mainLock.unlock();
                        }
                    }
                    
                    if (!isShutdown || !running) {
                        logger.info("[Worker] " + Thread.currentThread().getName() + 
                                  " executes " + task);
                        task.run();
                        logger.info("[Worker] " + Thread.currentThread().getName() + 
                                  " completed " + task);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception e) {
                    logger.warning("[Worker] " + Thread.currentThread().getName() + 
                                 " error: " + e.getMessage());
                }
            }
            
            mainLock.lock();
            try {
                activeThreads.decrementAndGet();
                workers.remove(this);
                logger.info("[Worker] " + Thread.currentThread().getName() + " terminated");
                if (activeThreads.get() == 0) {
                    terminationCondition.signalAll();
                }
            } finally {
                mainLock.unlock();
            }
        }
    }
    
    private static class CustomThreadFactory implements ThreadFactory {
        private final AtomicInteger threadNumber = new AtomicInteger(1);
        
        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r, "MyPool-worker-" + threadNumber.getAndIncrement());
            t.setDaemon(false);
            logger.info("[ThreadFactory] Creating new thread: " + t.getName());
            t.setUncaughtExceptionHandler((th, ex) -> 
                logger.severe("[ThreadFactory] Thread " + th.getName() + " died: " + ex));
            return t;
        }
    }
}

interface CustomExecutor extends Executor {
    <T> Future<T> submit(Callable<T> callable);
    void shutdown();
    void shutdownNow();
}

interface RejectedExecutionHandler {
    void rejectedExecution(Runnable task, CustomThreadPool executor);
}