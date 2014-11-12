package com.jobquest.process;

import java.io.*;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
 
import java.text.SimpleDateFormat;
import java.util.Date;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class ProcessQueueThread implements Runnable {

    private BlockingQueue<String> queue;
    private String outpath;
  
    public boolean isalive = true;

    public ProcessQueueThread(BlockingQueue<String> bqueue, String opath){
        this.queue = bqueue;
        this.outpath = opath;
    }

    @Override
    public void run(){
         int     maxlen, i, filenum;
         boolean notdone = true;
         String  wrapped, tobj;

         try {
             while(notdone){
                 tobj = queue.take();
                 if ( tobj.equals("DONE") ){
                    notdone = isalive = false;
                 } else {
                    i = tobj.indexOf(" ");
                    if ( i > -1 ){
                        filenum = Integer.parseInt(tobj.substring(0,i));
                        tobj = tobj.substring(i+1);
                        i = tobj.indexOf(" ");
                        if ( i > -1 ){
                            StringBuffer stbuf = new StringBuffer();
                            maxlen = Integer.parseInt(tobj.substring(0, i));
                            wrapped = wrap(tobj.substring(i+1), maxlen);
                            stbuf.append(maxlen);
                            stbuf.append(" ");
                            stbuf.append(wrapped);
                            try {
                              PrintWriter writer = new PrintWriter(
                                     outpath+"/Telegram_"+filenum+".txt"); 
                              writer.println(stbuf.toString());
                              writer.close();
                            }catch(IOException ioex){
                              // Ideally if we use log4j, we can log error messages to keep track of 
                              // the problems. Right here, I am just using basic messagedisplay
                            }
                            stbuf = null;
                        }
                    }
                 }
             }
         }catch(InterruptedException itx) { 
            System.out.println("Interrupred Writing Thread"); 
         } 
    }

   private String wrap(String str, int wrapLength) {
        if (str == null) {
            return null;
        }
        String newLineStr = System.getProperty("line.separator");
        if (wrapLength < 1) {
            wrapLength = 1;
        }
        int inputLineLength = str.length();
        int offset = 0;
        StringBuilder wrappedLine = new StringBuilder(inputLineLength + 32);

        while ((inputLineLength - offset) > wrapLength) {
            if (str.charAt(offset) == ' ') {
                offset++;
                continue;
            }
            int spaceToWrapAt = str.lastIndexOf(' ', wrapLength + offset);
            if (spaceToWrapAt >= offset) {
                // normal case
                wrappedLine.append(str.substring(offset, spaceToWrapAt));
                wrappedLine.append(newLineStr);
                offset = spaceToWrapAt + 1;

            } else {
                // wrap really long word one line at a time
                wrappedLine.append(str.substring(offset, wrapLength + offset));
                wrappedLine.append(newLineStr);
                offset += wrapLength;
            }
        }
        // Whatever is left in line is short enough to just pass through
        wrappedLine.append(str.substring(offset));
        return wrappedLine.toString();
  }  

}
