package com.bic.migration.util;

import java.io.BufferedWriter;
import java.io.IOException;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RecursiveAction;

public class FileWriter extends RecursiveAction {
    private Path fileToLog = null;
    private int initQCap = 0;
    BlockingQueue<String> dataQueue = null;
    private static final String SHUTDOWN_REQ = "SHUTDOWN";
    private volatile boolean shuttingDown, loggerTerminated;

    public FileWriter() {
        super();
    }

    public FileWriter(int initQCap, String file) {
        super();
        this.initQCap = initQCap;
        this.dataQueue = new LinkedBlockingQueue<String>(initQCap);
        if (file != null && file.trim().length() > 0) {
            fileToLog = Paths.get(file);
        }
    }

    public void run() {
        System.out.println("FileWriter Started. Thread id: " + Thread.currentThread().getName());

        String str = null;

        try {
            while ((str = dataQueue.take()) != SHUTDOWN_REQ) {
                writeToFileFrmQueue(str);
            }
        } catch (InterruptedException e) {
            System.out.println("Thread interrupted " + Thread.currentThread().getName());
            e.printStackTrace();
        } finally {
            loggerTerminated = true;
        }
    }

    public void log(String str) {
        if (shuttingDown || loggerTerminated)
            return;
        try {
            dataQueue.put(str);
        } catch (InterruptedException iex) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Unexpected interruption");
        }
    }

    public void shutDown() throws InterruptedException {
        shuttingDown = true;
        dataQueue.put(SHUTDOWN_REQ);
    }

    public boolean isShuttingDown() {
        return shuttingDown;
    }

    public boolean isLoggerTerminated() {
        return loggerTerminated;
    }

    private void writeToFileFrmQueue(String logEntry) {
        try (BufferedWriter out =
             Files.newBufferedWriter(fileToLog, StandardCharsets.UTF_8, StandardOpenOption.CREATE,
                                     StandardOpenOption.APPEND)) {
            out.append(logEntry);
            out.newLine();
        } catch (IOException e) {
            System.out.println("Error opening BufferedWriter for writing file.");
            e.printStackTrace();
        }
    }

    protected void compute() {
        System.out.println("FileWriter Started. Thread id: " + Thread.currentThread().getName());

        String str = null;

        try {
            while ((str = dataQueue.take()) != SHUTDOWN_REQ) {
                writeToFileFrmQueue(str);
            }
        } catch (InterruptedException e) {
            System.out.println("Thread interrupted " + Thread.currentThread().getName());
            e.printStackTrace();
        } finally {
            loggerTerminated = true;
        }
    }
}
