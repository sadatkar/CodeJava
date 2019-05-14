package com.bic.migration.util;

import java.io.IOException;

import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;

import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.regex.Pattern;

import javax.sql.rowset.Predicate;


public class DirectoryIterator implements FileVisitor<Path> {
    Path rootDir = null;
    Pattern pattern = null;
    Deque<String> dirs = new LinkedList<String>();
    Deque<String> files = new LinkedList<String>();
    

    public DirectoryIterator() {
        super();
    }


    public static void main(String[] args) {
        DirectoryIterator directoryIterator = new DirectoryIterator();
 
        try {
            //directoryIterator.iterate(Paths.get("W:/82/2014/NOV"), "FINALART");
            //directoryIterator.walk(Paths.get("W:/82/2014/NOV"), "FINALART");
            Pattern pattern = Pattern.compile("\\d{5,}");
            //System.out.println(pattern.matcher("01272").matches());
            System.out.println(" 285868-  ARTFN-1-BARREL-RED   186.PDF".replaceAll("\\s+", ""));
        } catch (Exception e) {
            System.err.println(e);
        }
    }
    
    public void walk(Path rtDir, String ptrnStr) throws IOException{
//        Files.walk(new File("W:/82/2014/NOV").toPath()).filter(p -> !p.getFileName().toString().startsWith(".")).forEach(System.out::println);
        pattern = Pattern.compile(ptrnStr);
        Files.walk(rtDir).filter((p) -> (pattern.matcher(p.toString()).find()) && Files.isRegularFile(p)).forEach((r) -> files.add(r.toString()));
    }
    
    
    public void iterate (Path rtDir, String ptrnStr) throws IOException {
        rootDir = rtDir;
        pattern = Pattern.compile(ptrnStr);
        Files.walkFileTree(rootDir, this);
        
//        Iterator dirIter = dirs.iterator();
//        while (dirIter.hasNext()) {
//            System.out.println("MATCHED_DIR: " + (String)dirIter.next());
//        }
        
        Iterator fileIter = files.iterator();
        while (fileIter.hasNext()) {
            System.out.println("MATCHED_FILE: " + (String)fileIter.next());
        }
    }

    @Override
    public FileVisitResult preVisitDirectory(Path path,
                                             BasicFileAttributes atts) throws IOException {        
//        boolean found= pattern.matcher(path.toString()).find();
//        if (found) {
//            //System.out.println("MATCHED_DIR: " + path.toString());
//            dirs.add(path.toString());
//        }
//
//        //return (found) ? FileVisitResult.CONTINUE : FileVisitResult.SKIP_SUBTREE;
        return FileVisitResult.CONTINUE;
    }

    @Override
    public FileVisitResult visitFile(Path path,
                                     BasicFileAttributes mainAtts) throws IOException {
        boolean found= pattern.matcher(path.toString()).find();
        if (found) {
            System.out.println("MATCHED_FILE: " + path.toString());
//            files.add(path.toString());
        }

        return FileVisitResult.CONTINUE;
    }

    @Override
    public FileVisitResult postVisitDirectory(Path path,
                                              IOException exc) throws IOException {
        // TODO Auto-generated method stub
        return FileVisitResult.CONTINUE;
    }

    @Override
    public FileVisitResult visitFileFailed(Path path,
                                           IOException exc) throws IOException {
        exc.printStackTrace();

        // If the root directory has failed it makes no sense to continue
        return path.equals(rootDir) ? FileVisitResult.TERMINATE :
               FileVisitResult.CONTINUE;
    }
}
