package com.jobquest.process;

import java.util.concurrent.*;

import java.text.SimpleDateFormat;
import java.util.Date;
 
public class MonitorRunningThread implements Runnable {

    // In order to check any one of the threads that 
    // has recognized END OF READ - by the marker "DONE"
    // in the queue
    private ProcessQueueThread pth[];

    // The handle to executor is required in order to shutdown
    // when the reading process is done
    private ThreadPoolExecutor executor;
     
    public MonitorRunningThread(ThreadPoolExecutor exct) {
       this.executor = exct;
    }
     
    public void setQueueArray(ProcessQueueThread pqth[]){
       this.pth = pqth;
    }

    @Override
    public void run() {
       int numq = pth.length;
       boolean completed = false;
       int i=0;
 
       while (!completed){
         i=0;
         while ( !completed && i < numq ){
          if ( pth[i].isalive == false  )
            completed = true;
          i++;
         }
         System.out.println("Monitoring all threads - are they complete "+completed);
         // Even if the threads have been completed, give some time
         // for any un-processed thread to finish up and then shutdown
         try {
           Thread.sleep(500);
         }catch(InterruptedException iex) { }
         if ( completed ){
           executor.shutdownNow();
           SimpleDateFormat ft = new SimpleDateFormat("hh:mm:ss a zzz");
           System.out.println("Ended ... @ "+ft.format(new Date()));
         }
       }
       
       
    }
}
