package com.bic.migration;

import com.bic.migration.util.FileWriter;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

import java.sql.SQLException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import oracle.stellent.ridc.IdcClient;
import oracle.stellent.ridc.IdcClientException;
import oracle.stellent.ridc.IdcClientManager;
import oracle.stellent.ridc.IdcContext;
import oracle.stellent.ridc.model.DataBinder;
import oracle.stellent.ridc.model.DataObject;
import oracle.stellent.ridc.model.DataResultSet;
import oracle.stellent.ridc.model.TransferFile;
import oracle.stellent.ridc.model.impl.DataObjectEncodingUtils;
import oracle.stellent.ridc.protocol.ServiceResponse;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 *
 * Migration POC code:
 * Terms: SalesOrder  - this is the human viewable ID for the order
 * Terms: Order_ID - this is the EBS system
 * Reads from Filesystem. Parses information from the NAME of the file.
 * Create a temp Document object (populates based on type of file)
 * Injects (via RIDC) document into WCC
 * Injects into WCC db table: AFOBJECTS the row that maps to EBS orderID
 *
 * @author skarim, scanlon
 *
 */
public class MigrationPOC {

    static Logger logger = LogManager.getLogger(MigrationPOC.class);

    static String SERVER = "idc://clwslwcc01d:4444";
    static int idcConnPoolSz = 20;
    static String FILTER_PAT = null;  
    static String REV_DIR_PAT = null;
    static String PO_PAT = null;
    
    static int initQCap = 10000;  
    static FileWriter successWriter = null;
    static FileWriter failureWriter = null;
    static FileWriter dataFailureWriter = null;


    /**
     * @param args
     * @throws IdcClientException
     * @throws IOException
     */
    public static void main(String[] args) throws IdcClientException, IOException, InterruptedException {

        logger.info("Process started at: " + new Date().toString());
        long stTm = System.currentTimeMillis();
        MigrationPOC poc = new MigrationPOC();
        boolean logging = false;
        int ttlFileCnt = 0;

        if (args != null && args.length > 0) {
            Map<String, String> envPropMap = poc.loadEnvProp(Paths.get(args[0]), "=");
            SERVER = envPropMap.get("SERVER");
            FILTER_PAT = envPropMap.get("FILTER_PAT");
            REV_DIR_PAT = envPropMap.get("REV_DIR_PATTRN");
            PO_PAT = envPropMap.get("PO_PAT");
            String baseDirStr = envPropMap.get("BASE_PATH");
            logging =
                envPropMap.get("LOG_ENBLD") != null && envPropMap.get("LOG_ENBLD").trim().length() > 0 ?
                new Boolean(envPropMap.get("LOG_ENBLD").trim()).booleanValue() : false;
            if (!logging) {
                Logger.getRootLogger().setLevel(Level.OFF);
            }
            logger.info("usage: pass in directory to start at");

            int degOfParallelism = Integer.valueOf(envPropMap.get("THREAD_POOL_SZ")).intValue();
            int lvlDepth = Integer.valueOf(envPropMap.get("LVL_DEPTH")).intValue();
            initQCap =
                envPropMap.get("INIT_Q_CAP").trim().length() > 0 ?
                Integer.valueOf(envPropMap.get("INIT_Q_CAP")).intValue() : initQCap;
            idcConnPoolSz =
                envPropMap.get("IDC_CONN_POOL_SZ").trim().length() > 0 ?
                Integer.valueOf(envPropMap.get("IDC_CONN_POOL_SZ")).intValue() : idcConnPoolSz;
            String[] prcdDirAry = envPropMap.get("PROCESSED_ELEMENTS").trim().length() > 0 ? envPropMap.get("PROCESSED_ELEMENTS").split(":") : null;
            String poLstFilePath = envPropMap.get("PO_LIST_FILE");

            successWriter = new FileWriter(initQCap, envPropMap.get("SUCCESS_LOG_FILE"));
            failureWriter = new FileWriter(initQCap, envPropMap.get("FAILURE_LOG_FILE"));
            dataFailureWriter = new FileWriter(initQCap, envPropMap.get("DATA_FAILURE_LOG_FILE"));
            logger.info("Before iterate");
            ForkJoinPool pool = new ForkJoinPool(3);
            pool.execute(successWriter);
            pool.execute(failureWriter);
            pool.execute(dataFailureWriter);
            ttlFileCnt = poc.iterate(baseDirStr, lvlDepth, degOfParallelism, prcdDirAry, poLstFilePath);
//            successWriter.join();
//            failureWriter.join();
//            dataFailureWriter.join();
            logger.info("AFter iterate");
        }

        long endTm = System.currentTimeMillis();
        if (logging) {
            logger.info("Process completed at: " + new Date().toString());
            logger.info("Total Files Processed: " + ttlFileCnt + ", Time Taken: " + (endTm - stTm) + " millis.");
        } else {
            System.out.println("Process completed at: " + new Date().toString());
            System.out.println("Total Files Processed: " + ttlFileCnt + ", Time Taken: " + (endTm - stTm) + " millis.");
        }
    }

    private Map<String, String> loadEnvProp(Path path, String ptrnStr) throws IOException {
        Map<String, String> propMap = new HashMap<String, String>();
        Files.lines(path).forEach((p) -> {
                String[] propAry = p.split(ptrnStr);
                propMap.put(propAry[0], propAry[1]);
            });

        return propMap;
    }

    private int iterate(String baseDirStr, int lvlDepth, int degOfParallel, String[] prcdDirs, String poLstFilePath) {
        int ttlFilePrcssed = 0;

        try {
            TreeSet<String> mnthDirSet = new TreeSet<String>();
            Pattern mnthPat = Pattern.compile(".?\\d{4}.?[A-Z]{3}");
            List<String> prcssdMnthLst = (prcdDirs != null && prcdDirs.length > 0) ? Arrays.asList(prcdDirs) : new ArrayList<String>();
            
            switch (lvlDepth) {
                //supplying directory upto month
            case 0:
                mnthDirSet.add(baseDirStr);
                ttlFilePrcssed = submitToPool(degOfParallel, mnthDirSet);
                break;
                //supplying directory upto year
            case 1:
                Path yrPath = Paths.get(baseDirStr);
                Files.walk(yrPath, 1,
                           FileVisitOption.FOLLOW_LINKS).filter((p) -> (mnthPat.matcher(p.toString()).find())).forEach((p) -> mnthDirSet.add(p.toString()));
                
                
                for (String prcssdMnthStr : prcssdMnthLst) {
                    mnthDirSet.remove(prcssdMnthStr);
                    System.out.println("DIR: " + prcssdMnthStr + " already processed. Skipping...");
                }
                ttlFilePrcssed = submitToPool(degOfParallel, mnthDirSet);
                break;
                //supplying root directory
            case 2:
                //1st find all directory upto months
                Path yrPath2 = Paths.get(baseDirStr);
                Files.walk(yrPath2, 2,
                           FileVisitOption.FOLLOW_LINKS).filter((p) -> (mnthPat.matcher(p.toString()).find())).forEach((p) -> mnthDirSet.add(p.toString()));

                //now find all PO specific directories inside each month directory and process in parallel.
                for (String prcssdMnthStr : prcssdMnthLst) {
                    mnthDirSet.remove(prcssdMnthStr);
                    System.out.println("DIR: " + prcssdMnthStr + " already processed. Skipping...");
                }
                ttlFilePrcssed = submitToPool(degOfParallel, mnthDirSet);
                break;
                //supplying PO list file that contains individual files
            case 3:
                if (poLstFilePath != null && poLstFilePath.trim().length() > 0) {
                    ttlFilePrcssed = submitToPoolPO(Paths.get(poLstFilePath), degOfParallel, false);
                }
                else {
                    System.out.println("Invalid PO processing parameter.");
                }                
                break;
                //supplying PO list file that contains PO directories
            default:
                if (poLstFilePath != null && poLstFilePath.trim().length() > 0) {
                    ttlFilePrcssed = submitToPoolPO(Paths.get(poLstFilePath), degOfParallel, true);
                }
                else {
                    System.out.println("Invalid PO processing parameter.");
                }                
                break;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        
        return ttlFilePrcssed;
    }

    /*private void submitToPool(Path path, String ptrnStr,
                              ForkJoinPool pool) throws java.util.concurrent.ExecutionException,
                                                        java.lang.InterruptedException {*/
    private int submitToPool(int degOfParallel, Set<String> mnthSet) throws java.util.concurrent.ExecutionException,
                                                            java.lang.InterruptedException {
        ForkJoinPool pool = new ForkJoinPool(degOfParallel);
        MonthProcessor mnthPrcssr = new MonthProcessor(null, REV_DIR_PAT, FILTER_PAT, PO_PAT, SERVER, idcConnPoolSz, mnthSet, successWriter, failureWriter, dataFailureWriter);
        pool.execute(mnthPrcssr);
        
        //Write to the console information about the status of the pool every second
        //until the three tasks have finished their execution.
        do
        {
           System.out.printf("******************************************\n");
           System.out.printf("Main: Parallelism: %d\n", pool.getParallelism());
           System.out.printf("Main: Active Threads: %d\n", pool.getActiveThreadCount());
           System.out.printf("Main: Task Count: %d\n", pool.getQueuedTaskCount());
           System.out.printf("Main: Steal Count: %d\n", pool.getStealCount());
           System.out.printf("******************************************\n");
            
           try
           {
              TimeUnit.SECONDS.sleep(1);
           } catch (InterruptedException e)
           {
              e.printStackTrace();
           }
        } while ((!mnthPrcssr.isDone()));
        
        mnthPrcssr.join();
        
        return mnthPrcssr.files;
    }
    
    private int submitToPoolPO(Path path, int degOfParallel, boolean isPODir) throws java.util.concurrent.ExecutionException,
                                                            java.lang.InterruptedException {
        int filPrcssd = 0;
        ForkJoinPool pool = new ForkJoinPool(degOfParallel);
        
        if (isPODir) {
            POProcessor poPrcssr = new POProcessor(path, REV_DIR_PAT, FILTER_PAT, PO_PAT, SERVER, idcConnPoolSz, successWriter, failureWriter, dataFailureWriter);
            pool.execute(poPrcssr);
            //Write to the console information about the status of the pool every second
            //until the three tasks have finished their execution.
            do
            {
               System.out.printf("******************************************\n");
               System.out.printf("Main: Parallelism: %d\n", pool.getParallelism());
               System.out.printf("Main: Active Threads: %d\n", pool.getActiveThreadCount());
               System.out.printf("Main: Task Count: %d\n", pool.getQueuedTaskCount());
               System.out.printf("Main: Steal Count: %d\n", pool.getStealCount());
               System.out.printf("******************************************\n");
                
               try
               {
                  TimeUnit.SECONDS.sleep(1);
               } catch (InterruptedException e)
               {
                  e.printStackTrace();
               }
            } while ((!poPrcssr.isDone()));
            
            poPrcssr.join();
            filPrcssd = poPrcssr.files;
        }
        else {
            POFileProcessor poFilPrcssr = new POFileProcessor(path, REV_DIR_PAT, FILTER_PAT, PO_PAT, SERVER, idcConnPoolSz, successWriter, failureWriter, dataFailureWriter);
            pool.execute(poFilPrcssr);
            //Write to the console information about the status of the pool every second
            //until the three tasks have finished their execution.
            do
            {
               System.out.printf("******************************************\n");
               System.out.printf("Main: Parallelism: %d\n", pool.getParallelism());
               System.out.printf("Main: Active Threads: %d\n", pool.getActiveThreadCount());
               System.out.printf("Main: Task Count: %d\n", pool.getQueuedTaskCount());
               System.out.printf("Main: Steal Count: %d\n", pool.getStealCount());
               System.out.printf("******************************************\n");
                
               try
               {
                  TimeUnit.SECONDS.sleep(1);
               } catch (InterruptedException e)
               {
                  e.printStackTrace();
               }
            } while ((!poFilPrcssr.isDone()));
            
            poFilPrcssr.join();
            filPrcssd = poFilPrcssr.files;
        }
                
        return filPrcssd;
    }
}
