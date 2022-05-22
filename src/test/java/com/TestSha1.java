package com;


import org.apache.commons.codec.digest.DigestUtils;

import java.io.File;

import static org.apache.commons.codec.digest.MessageDigestAlgorithms.SHA_1;

public class TestSha1 {
    public static void main(String[] args) throws Exception {
        File file = new File("D:\\java apps\\mavenLocalRepository\\com\\jd\\bdp\\hbase\\jdnosql-client\\3.1.8\\jdnosql-client-3.1.8.pom");
        String hex = new DigestUtils(SHA_1).digestAsHex(file);
        System.out.println(hex);
    }
}
