package com.bic.migration.util;

import com.bic.migration.Document;

import java.io.IOException;

import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.Path;

import java.nio.file.Paths;

import java.util.Arrays;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import java.util.stream.Stream;

import oracle.stellent.ridc.model.impl.DataObjectEncodingUtils;

public class Tester {
    public Tester() {
        super();
    }

    public static void main(String[] args) {
        Tester tester = new Tester();
//        //Pattern pattern = Pattern.compile("\\d{2}.?\\d{4}.?[A-Z]{3}");
//        Pattern pattern = Pattern.compile(".?\\d{4}.?[A-Z]{3}");
//        //Pattern pattern = Pattern.compile(".?[A-Z]{3}");
//        Path path = Paths.get("C:/Prod/BIC/Thread");
//        try {
//            Files.walk(path, 2,
//                       FileVisitOption.FOLLOW_LINKS).filter((p) -> (pattern.matcher(p.toString()).find())).forEach((p) -> System.out.println(p.toString()));
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
        //String pos ="PO:100718059 files = /app/Oracle/Middleware/Archive/82/2014/APR/100718059/ARCHIVE/FINALART/100718059-ARTFN-1-SIDE 1-METALLIC GREEN 8283.PDF, /app/Oracle/Middleware/Archive/82/2014/APR/100718059/ARCHIVE/FINALART/100718059-ARTFN-10-SIDE 1-METALLIC GREEN 8283.PDF, /app/Oracle/Middleware/Archive/82/2014/APR/100718059/ARCHIVE/FINALART/100718059-ARTFN-11-SIDE 1-METALLIC GREEN 8283.PDF, /app/Oracle/Middleware/Archive/82/2014/APR/100718059/ARCHIVE/FINALART/100718059-ARTFN-12-SIDE 1-METALLIC GREEN 8283.PDF, /app/Oracle/Middleware/Archive/82/2014/APR/100718059/ARCHIVE/FINALART/100718059-ARTFN-13-SIDE 1-METALLIC GREEN 8283.PDF, /app/Oracle/Middleware/Archive/82/2014/APR/100718059/ARCHIVE/FINALART/100718059-ARTFN-14-SIDE 1-METALLIC GREEN 8283.PDF, /app/Oracle/Middleware/Archive/82/2014/APR/100718059/ARCHIVE/FINALART/100718059-ARTFN-15-SIDE 1-METALLIC GREEN 8283.PDF, /app/Oracle/Middleware/Archive/82/2014/APR/100718059/ARCHIVE/FINALART/100718059-ARTFN-16-SIDE 1-METALLIC GREEN 8283.PDF, /app/Oracle/Middleware/Archive/82/2014/APR/100718059/ARCHIVE/FINALART/100718059-ARTFN-17-SIDE 1-METALLIC GREEN 8283.PDF, /app/Oracle/Middleware/Archive/82/2014/APR/100718059/ARCHIVE/FINALART/100718059-ARTFN-18-SIDE 1-METALLIC GREEN 8283.PDF, /app/Oracle/Middleware/Archive/82/2014/APR/100718059/ARCHIVE/FINALART/100718059-ARTFN-19-SIDE 1-METALLIC GREEN 8283.PDF, /app/Oracle/Middleware/Archive/82/2014/APR/100718059/ARCHIVE/FINALART/100718059-ARTFN-2-SIDE 1-METALLIC GREEN 8283.PDF, /app/Oracle/Middleware/Archive/82/2014/APR/100718059/ARCHIVE/FINALART/100718059-ARTFN-20-SIDE 1-METALLIC GREEN 8283.PDF, /app/Oracle/Middleware/Archive/82/2014/APR/100718059/ARCHIVE/FINALART/100718059-ARTFN-3-SIDE 1-METALLIC GREEN 8283.PDF, /app/Oracle/Middleware/Archive/82/2014/APR/100718059/ARCHIVE/FINALART/100718059-ARTFN-4-SIDE 1-METALLIC GREEN 8283.PDF, /app/Oracle/Middleware/Archive/82/2014/APR/100718059/ARCHIVE/FINALART/100718059-ARTFN-5-SIDE 1-METALLIC GREEN 8283.PDF, /app/Oracle/Middleware/Archive/82/2014/APR/100718059/ARCHIVE/FINALART/100718059-ARTFN-6-SIDE 1-METALLIC GREEN 8283.PDF, /app/Oracle/Middleware/Archive/82/2014/APR/100718059/ARCHIVE/FINALART/100718059-ARTFN-7-SIDE 1-METALLIC GREEN 8283.PDF, /app/Oracle/Middleware/Archive/82/2014/APR/100718059/ARCHIVE/FINALART/100718059-ARTFN-8-SIDE 1-METALLIC GREEN 8283.PDF, /app/Oracle/Middleware/Archive/82/2014/APR/100718059/ARCHIVE/FINALART/100718059-ARTFN-9-SIDE 1-METALLIC GREEN 8283.PDF";
        Path path = Paths.get("/app/Oracle/Middleware/WCC_HOT_FOLDER/82/2015/MAY/123456789/ADMIN/100037550-CUSTART-02.PDF");
        //Path path = Paths.get("Z:/Archive/82/2012/APR/G90419/FINALART/G90419.INDD");
        try {
            //Files.lines(path).map((p) -> parseLn(p, "=")).forEach((q) -> {String[] filAry = q.trim().split(","); Arrays.asList(filAry).forEach((r) -> {Path dir = Paths.get(r.trim()); System.out.println(dir.toFile().getPath());});});
//            Pattern ptrn = Pattern.compile("ADMIN");
            Pattern patNum = Pattern.compile("\\d{9,}");
            Pattern patChar = Pattern.compile("\\D{1,}");
            //Pattern patMixed = Pattern.compile("\\d{9,}|[A-Za-z]\\d{5,}");
            Pattern patMixed = Pattern.compile("\\d{9,}|[A-Za-z]\\d{5,}|\\d{6,}");
            Matcher m = patMixed.matcher("/app/Oracle/Middleware/WCC_HOT_FOLDER/82/2015/MAY/123456789/ADMIN/100037550-CUSTART-02.PDF");
            //Matcher m = patMixed.matcher("Z:/Archive/82/2012/APR/G90419/FINALART/G90419.INDD");
//            Document doc = new Document();
//            doc.initIdxDocTypLst();
//            doc.initIdxDocTypMap();
//            System.out.println("Matched:" + doc.getPONum("100288611-CUSTPO.PDF", "Z:/Archive/82/2013/APR/100288611/ADMIN/"));
//            System.out.println("Matched:" + doc.getIdxDocTyp("100288611-CUSTPO.PDF", "Z:/Archive/82/2013/APR/100288611/ADMIN/"));
            ///app/Oracle/Middleware/Archive/82/2014/APR/100700784/ADMIN/Shortcut to ADMIN.lnk
            if (m.find()) {
                String po = m.group();
                System.out.println("Matched:" + po);
//                String po = m.group().replaceAll("\\D", "");
//                System.out.println("Matched:" + po);
//                m.find();
//                String po1 = m.group().replaceAll("\\D", "");
//                System.out.println("Matched:" + po1);
                String fullPath = path.toString();
                System.out.println("FullPath:" + fullPath);
                System.out.println("Idx:" + fullPath.indexOf(po) + "#" + po.length() + "#" + fullPath.lastIndexOf("\\"));
                //System.out.println("SubPath:" + path.subpath(path.toString().indexOf(po) + po.length(), path.toString().lastIndexOf("\\")).toString());
                //Path p = path.subpath(4, 6);
                System.out.println("SubPath:" + fullPath.substring(path.toString().indexOf(po) + po.length(), path.toString().lastIndexOf("\\")));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
    
    private static String parseLn(String ln, String pat) {
        String[] poAry = ln.split(pat); 
        String filLns = poAry[1].trim();
        return filLns;
    }
    
    public static String[] fileNameParse(String in) {
        //              100000397-ARTFN-1-SURFACE-CMYK-001.EPS
        return in.substring(0, in.lastIndexOf(".")).split("-");
        //      return in.split("-");

    }
}
