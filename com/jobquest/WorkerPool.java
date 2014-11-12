package com.jobquest;

import java.util.*;
import java.util.concurrent.*;

import java.io.File;

import com.jobquest.io.ReadFileWithMappedByteBuffer;
import com.jobquest.TelegramObject;
import com.jobquest.RejectedExecutionHandlerImpl;

import com.jobquest.process.ProcessQueueThread;
import com.jobquest.process.MonitorRunningThread;

import java.text.SimpleDateFormat;
import java.util.Date;

public class WorkerPool {

   public static void main(String[] args){
       int numargs = args.length;
       boolean hasinp  = false;
       boolean hasoutp = false;
       File f;

       switch(numargs){
         case 0:  
         case 1:
                  System.out.println("java com.jobquest.WorkerPool <input-file-path> <output-directory-path>");
                  break;
         case 2:  
                  f = new File(args[0]);
                  if ( f.exists() ){
                    hasinp = true;
                  } else {
                    System.out.println("The file "+args[0]+" does not exist");
                  }
                  if ( new File(args[1]).exists() )
                    hasoutp = true;
                  else
                    System.out.println("The output file path "+args[1]+" does not exist");

                  if ( hasinp && hasoutp )
                    runworker(args[0], args[1]);
                  break;
       }
   }

   public static void runworker(String filename, String opath){
      String outputFilePath = opath;

      //RejectedExecutionHandler implementation
      RejectedExecutionHandlerImpl rejectionHandler = new RejectedExecutionHandlerImpl();

      //Get the ThreadFactory implementation to use
      ThreadFactory threadFactory = Executors.defaultThreadFactory();
    
      //creating the ThreadPoolExecutor (which is resposible for executing threads)
      ThreadPoolExecutor executorPool = new ThreadPoolExecutor(10, 10, 10, TimeUnit.SECONDS, 
            new LinkedBlockingQueue<Runnable>(10), threadFactory, rejectionHandler);

      // This queue is handling the lines read from large file
      BlockingQueue<String> queue = new LinkedBlockingQueue<String>(1024);

      // The file reads can only happen sequentially - but using Java NIO API's one can
      // read files much faster using buffers
      ReadFileWithMappedByteBuffer rfbuffer = new ReadFileWithMappedByteBuffer(filename, queue); 

      SimpleDateFormat ft = new SimpleDateFormat("hh:mm:ss a zzz");
      System.out.println("Started ... @ "+ft.format(new Date()));

      try {

         MonitorRunningThread monitor = new MonitorRunningThread(executorPool);
         ProcessQueueThread pqth[] = new ProcessQueueThread[10];
         
         //submit work to the thread pool
         for(int i=0; i<10; i++){
               pqth[i] = new ProcessQueueThread(queue, outputFilePath);
               executorPool.execute(pqth[i]);
         }

         rfbuffer.run();

         // monitor will keep track when the job ends and exit the main process
         monitor.setQueueArray(pqth);
         monitor.run();

      }catch(Exception exc){
      }
   }

}
