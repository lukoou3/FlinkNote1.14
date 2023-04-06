package com.java.connector.test;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.SequenceReader;
import org.junit.Test;

import java.io.*;
import java.util.List;

public class IoTest {

    @Test
    public void geneFile()  throws Exception{
        File file = new File("D:\\file\\in.txt");
        FileOutputStream outputStream = new FileOutputStream(file, false);
        //byte[] bytes = new byte[1024 * 1024];
        byte[] bytes = new byte[4];
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = 97;
        }
        outputStream.write(bytes);
        outputStream.close();
        System.out.println(11);
    }

    @Test
    public void testCopy() throws Exception {
        File srcFile = new File("D:\\file\\in.txt");
        File descFile = new File("D:\\file\\out.txt");
        FileInputStream inputStream1 = new FileInputStream(srcFile);
        FileInputStream inputStream2 = new FileInputStream(srcFile);
        FileOutputStream outputStream = new FileOutputStream(descFile, false);
        IOUtils.copy(inputStream1, outputStream);
        IOUtils.copy(inputStream2, outputStream);
        inputStream1.close();
        inputStream2.close();
        outputStream.close();
    }

    @Test
    public void testSequenceInputStream() throws Exception {
        File srcFile = new File("D:\\file\\in.txt");
        File descFile = new File("D:\\file\\out.txt");
        InputStream inputStream = new SequenceInputStream(new FileInputStream(srcFile), new FileInputStream(srcFile));
        FileOutputStream outputStream = new FileOutputStream(descFile, false);
        IOUtils.copy(inputStream, outputStream);
        inputStream.close();
        outputStream.close();
    }

    @Test
    public void tesSequenceReader() throws Exception {
        File srcFile = new File("D:\\file\\in.txt");
        Reader reader = new SequenceReader(new FileReader(srcFile), new FileReader(srcFile), new FileReader(srcFile));
        List<String> lines = IOUtils.readLines(reader);
        System.out.println(lines);
    }

}
