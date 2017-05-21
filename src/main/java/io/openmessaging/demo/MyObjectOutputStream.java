package io.openmessaging.demo;

import java.io.File;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;

public class MyObjectOutputStream extends ObjectOutputStream {  
    public MyObjectOutputStream() throws SecurityException, IOException{
    	super();
    }
    public MyObjectOutputStream(OutputStream out) throws IOException{
    	super(out);
    }
    @Override
    protected void writeStreamHeader() throws IOException{
    	return;
    }
} 