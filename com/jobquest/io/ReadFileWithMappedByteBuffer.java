package com.jobquest.io;

import java.io.*;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
 
import java.text.SimpleDateFormat;
import java.util.Date;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class ReadFileWithMappedByteBuffer implements Runnable
{

    // Input file name - which is expected to be large size
    private String input_filename;

    // Using RandomAccessFile to read large file
    private RandomAccessFile aFile;

    // Use FileChannel and buffer to read large file
    private FileChannel inChannel;

    // In order to generated telegram files by index number, need a linecount
    private long linecount=0;
    
    // This queue holds the line read from file - which is processed by threads to 
    // create telegrams
    private BlockingQueue<String> queue;

    public ReadFileWithMappedByteBuffer(String fname, BlockingQueue<String> bqueue){
        this.input_filename = fname;
        this.queue = bqueue;
    }

    private void readBufferAndQueue(byte[] buffer){
       BufferedReader in;
       String line=null;
       try {
         in = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(buffer)));
         for (line = in.readLine(); line != null; line = in.readLine()) {
             queue.put(linecount+" " +line);
             linecount++;
             line = null;
         }
       }catch(IOException ioex) {
          // Ideally if we use log4j, we can log error messages to keep track of 
          // the problems. Right here, I am just using basic messagedisplay
          System.out.println("IOEX:: "+ioex.getMessage());
       }catch(InterruptedException itx) {
          System.out.println("ITX::" + itx.getMessage());
       }
    }

    public void run() {
       MappedByteBuffer buffer = null;
       try {
        aFile = new RandomAccessFile(input_filename, "r");
        inChannel = aFile.getChannel();
        int maxsize = Integer.MAX_VALUE;

        long remaining_size;
        long channel_size = inChannel.size();
        long num_buffers = channel_size / maxsize;
        if ( maxsize * num_buffers < channel_size ) num_buffers++;

        // There might be memory limitation that might lead to
        // failure - and therefore we need to consider the situation
        // where the channel size is too large - in which case break down
        // the reading process into chunks
        if ( num_buffers == 1 ){
          buffer = inChannel.map(FileChannel.MapMode.READ_ONLY, 0, channel_size);
          byte[] charbuffer = new byte[(int)channel_size];
          buffer.get(charbuffer);
          System.out.println("In reading thread");
          readBufferAndQueue(charbuffer);
          buffer.clear(); 
        } else {
          maxsize = 92160;
          channel_size = inChannel.size();
          num_buffers = channel_size / maxsize;
          remaining_size = maxsize;
          while ( remaining_size > 0 ){
            buffer = inChannel.map(FileChannel.MapMode.READ_ONLY, 0, remaining_size);
            byte[] charbuffer = new byte[(int)remaining_size];
            buffer.get(charbuffer);
            readBufferAndQueue(charbuffer);
            buffer.clear(); 
            charbuffer = null;
            channel_size = channel_size - remaining_size;
            if ( channel_size > maxsize )
                remaining_size = maxsize;
            else
                remaining_size = channel_size;
            //System.out.println("linecount = "+linecount+" channel size = "+channel_size+" and remaining size="+remaining_size);
            charbuffer = null;
          }
        }
       }catch(IOException ioex) { 
       }finally {
         try{
          inChannel.close();
          aFile.close();
          // This marks the end of process
          queue.put("DONE");
         }catch(IOException ioex) { 
         }catch(InterruptedException itx) { 
         }
       }
    }
}
