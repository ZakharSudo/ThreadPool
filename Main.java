import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class Main {
    public static void main(String[] args) throws InterruptedException {
        System.setProperty("java.util.logging.SimpleFormatter.format", "[%1$tT] %4$s: %5$s%n");
        
        System.out.println("=== Демонстрация CustomThreadPool ===\n");
        
        CustomThreadPool pool = new CustomThreadPool.Builder()
            .corePoolSize(2)
            .maxPoolSize(4)
            .keepAliveTime(3, TimeUnit.SECONDS)
            .queueSize(3)
            .minSpareThreads(1)
            .rejectedPolicy(CustomThreadPool.RejectedPolicy.CALLER_RUNS)
            .build();
        
        System.out.println("\n--- Отправка 10 задач с задержкой 1 сек ---");
        for (int i = 1; i <= 10; i++) {
            final int taskId = i;
            try {
                pool.execute(() -> {
                    System.out.println(Thread.currentThread().getName() + 
                                     " starting task " + taskId);
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    System.out.println(Thread.currentThread().getName() + 
                                     " finished task " + taskId);
                });
                Thread.sleep(100);
            } catch (RejectedExecutionException e) {
                System.out.println("Task " + taskId + " rejected: " + e.getMessage());
            }
        }
        
        Thread.sleep(5000);
        
        System.out.println("\n--- Тест переполнения (быстрая отправка 20 задач) ---");
        for (int i = 11; i <= 30; i++) {
            final int taskId = i;
            pool.execute(() -> {
                System.out.println(Thread.currentThread().getName() + " process task " + taskId);
                try { Thread.sleep(500); } catch (InterruptedException e) {}
            });
        }
        
        Thread.sleep(3000);
        
        System.out.println("\n--- Callable задачи ---");
        Future<Integer> future = pool.submit(() -> {
            Thread.sleep(500);
            return 42;
        });
        
        try {
            Integer result = future.get(1, TimeUnit.SECONDS);
            System.out.println("Callable result: " + result);
        } catch (TimeoutException e) {
            System.out.println("Callable timeout");
        } catch (InterruptedException e) {
            System.out.println("Callable interrupted: " + e.getMessage());
        } catch (ExecutionException e) {
            System.out.println("Callable execution error: " + e.getCause().getMessage());
        }
        
        System.out.println("\n--- Shutdown ---");
        pool.shutdown();
        
        try {
            pool.execute(() -> System.out.println("This should be rejected"));
        } catch (RejectedExecutionException e) {
            System.out.println("Correctly rejected after shutdown");
        }
        
        boolean terminated = pool.awaitTermination(10, TimeUnit.SECONDS);
        System.out.println("Pool terminated: " + terminated);
        
        System.out.println("\n=== Тестирование политик отказа ===");
        
        System.out.println("\n--- Политика ABORT ---");
        CustomThreadPool abortPool = new CustomThreadPool.Builder()
            .corePoolSize(1).maxPoolSize(1).queueSize(1)
            .rejectedPolicy(CustomThreadPool.RejectedPolicy.ABORT).build();
        
        for (int i = 0; i < 5; i++) {
            try {
                abortPool.execute(() -> { 
                    try { Thread.sleep(100); } catch (InterruptedException e) {} 
                });
            } catch (RejectedExecutionException e) {
                System.out.println("Rejected with ABORT");
                break;
            }
        }
        abortPool.shutdown();
        
        System.out.println("\n--- Политика DISCARD ---");
        CustomThreadPool discardPool = new CustomThreadPool.Builder()
            .corePoolSize(1).maxPoolSize(1).queueSize(1)
            .rejectedPolicy(CustomThreadPool.RejectedPolicy.DISCARD).build();
        
        for (int i = 0; i < 5; i++) {
            final int id = i;
            discardPool.execute(() -> System.out.println("Task " + id + " executed"));
        }
        Thread.sleep(1000);
        discardPool.shutdown();
        
        System.out.println("\n=== Демонстрация завершена ===");
    }
}