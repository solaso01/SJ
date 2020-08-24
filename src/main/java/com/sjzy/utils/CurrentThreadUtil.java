package com.sjzy.utils;


import java.util.concurrent.*;


public class CurrentThreadUtil {
	
    /**
     * @Title: parallelJob   
     * @Description: 多线程并发处理
     * @param command
     * @param bq
     * @param threadNums      
     * @throws
     */
    public static void parallelJob(final IParallelThread command,
                                   final BlockingQueue<Object> bq,
                                   int threadNums) {
        ExecutorService es = Executors.newFixedThreadPool(threadNums > 0 ? threadNums : Runtime.getRuntime().availableProcessors() * 2);
        CompletionService<Boolean> cs = new ExecutorCompletionService<Boolean>(es);
        int len = bq.size();
        while (!bq.isEmpty()) {
            cs.submit(new Callable<Boolean>() {
                Object obj = bq.poll();
                @Override
                public Boolean call() throws Exception {
                    return command.doMyJob(obj);
                }
            });
        }
        for (int i = 0; i < len; i++) {
            try {
                Future<Boolean> res = cs.take();
                if (res.get().equals(Boolean.FALSE)) {
                    throw new RuntimeException("CurrentThreadUtil-多线程执行出错："+i);
                }
            } catch (Exception e) {
                throw new RuntimeException("CurrentThreadUtil-多线程执行出错"+e.getMessage(), e);
            }
        }
        es.shutdown();
    }
}
