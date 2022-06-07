package com;


import org.apache.commons.codec.digest.DigestUtils;

import java.io.File;

import static org.apache.commons.codec.digest.MessageDigestAlgorithms.SHA_1;

public class TestSha1 {
    public static void main(String[] args) throws Exception {
        File file = new File("D:\\java-apps\\mavenRepository\\org\\apache\\flink\\flink-table-api-scala-bridge_2.11\\1.14.4\\flink-table-api-scala-bridge_2.11-1.14.4-sources.jar");
        String hex = new DigestUtils(SHA_1).digestAsHex(file);
        System.out.println(hex);
    }
}
