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
import java.util.TreeSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import java.util.stream.Stream;

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
public class MigrationPOCDelta {

    private static Logger logger = LogManager.getLogger(MigrationPOCDelta.class);

    private static int runStarter = (int) System.currentTimeMillis();

    private static String SERVER = null;
    private static String BIC_SITE = null;
    private static volatile IdcClientManager mIdcClientMgr;
    private static int initQCap = 10000;
    private static int idcConnPoolSz = 20;
    private static FileWriter successWriter = null;
    private static FileWriter failureWriter = null;
    private static FileWriter dataFailureWriter = null;

    static int folders = 0;
    static int files = 0;

    /**
     * @param args
     * @throws IdcClientException
     * @throws IOException
     */
    public static void main(String[] args) throws IdcClientException, IOException, InterruptedException {

        logger.info("Process started at: " + new Date().toString());
        long stTm = System.currentTimeMillis();
        MigrationPOCDelta poc = new MigrationPOCDelta();
        boolean logging = false;

        if (args != null && args.length > 0) {
            Map<String, String> envPropMap = poc.loadEnvProp(Paths.get(args[0]), "=");
            SERVER = envPropMap.get("SERVER");
            BIC_SITE = envPropMap.get("BIC_SITE");
            Path baseDir = Paths.get(envPropMap.get("BASE_PATH"));
            logging =
                envPropMap.get("LOG_ENBLD") != null && envPropMap.get("LOG_ENBLD").trim().length() > 0 ? new Boolean(envPropMap.get("LOG_ENBLD").trim()).booleanValue() : false;
            boolean isDeltaPO = envPropMap.get("IS_DELTAPO") != null && envPropMap.get("IS_DELTAPO").trim().length() > 0 ? new Boolean(envPropMap.get("IS_DELTAPO").trim()).booleanValue() : false;
            if (!logging) {
                Logger.getRootLogger().setLevel(Level.OFF);
            }
            logger.info("usage: pass in directory to start at");

            int degOfParallelism = Integer.valueOf(envPropMap.get("THREAD_POOL_SZ")).intValue();
            int lvlDepth = Integer.valueOf(envPropMap.get("LVL_DEPTH")).intValue();
            initQCap = envPropMap.get("INIT_Q_CAP").trim().length() > 0 ? Integer.valueOf(envPropMap.get("INIT_Q_CAP")).intValue() : initQCap;
            idcConnPoolSz = envPropMap.get("IDC_CONN_POOL_SZ").trim().length() > 0 ? Integer.valueOf(envPropMap.get("IDC_CONN_POOL_SZ")).intValue() : idcConnPoolSz;
            String[] prcdDirAry = args.length == 9 && envPropMap.get("PROCESSED_ELEMENTS").trim().length() > 0 ? envPropMap.get("PROCESSED_ELEMENTS").split(":") : null;

            successWriter = new FileWriter(initQCap, envPropMap.get("SUCCESS_LOG_FILE"));
            failureWriter = new FileWriter(initQCap, envPropMap.get("FAILURE_LOG_FILE"));
            dataFailureWriter = new FileWriter(initQCap, envPropMap.get("DATA_FAILURE_LOG_FILE"));
            logger.info("Before iterate");
            ForkJoinPool pool = new ForkJoinPool(2);
            pool.execute(successWriter);
            pool.execute(failureWriter);
            pool.execute(dataFailureWriter);
            poc.iterate(baseDir, envPropMap.get("REV_DIR_PATTRN"), degOfParallelism, isDeltaPO);
            logger.info("AFter iterate");
        }

        long endTm = System.currentTimeMillis();
        if (logging) {
            logger.info("Process completed at: " + new Date().toString());
            logger.info("Total Files Processed: " + files + ", Time Taken: " + (endTm - stTm) + " millis.");
        } else {
            System.out.println("Process completed at: " + new Date().toString());
            System.out.println("Total Files Processed: " + files + ", Time Taken: " + (endTm - stTm) + " millis.");
        }
    }
    
    private Map<String, String> loadEnvProp (Path path, String ptrnStr) throws IOException {
        Map<String, String> propMap = new HashMap<String, String>();
        Files.lines(path).forEach((p) -> {String[] propAry = p.split(ptrnStr); propMap.put(propAry[0], propAry[1]);});
        
        return propMap;
    }

    private void iterate(Path path, String ptrnStr, int degOfParallel, boolean isDeltaPO) {
        ForkJoinPool pool = new ForkJoinPool(degOfParallel);

        try {
            submitToPool(path, ptrnStr, pool, isDeltaPO);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void submitToPool(Path path, String ptrnStr,
                              ForkJoinPool pool, boolean isDeltaPO) throws java.util.concurrent.ExecutionException,
                                                        java.lang.InterruptedException {
        pool.submit(() -> { try {
                processParallel(path, ptrnStr, isDeltaPO);
                System.out.println("Processing DIR: " + path.getFileName());
            } catch (IOException e) {
                System.out.println("Failed to invoke processParallel: \n");
                e.printStackTrace();
            } }).get();
    }

    private void processParallel(Path path, String ptrnStr, boolean isDeltaPO) throws java.io.IOException {
        Files.lines(path).parallel().map((p) -> parseLn(p, "=")).forEach((q) -> {
                String[] filAry = q.trim().split(",");
                Arrays.asList(filAry).forEach((r) -> { process(Paths.get(r.trim()), ptrnStr, isDeltaPO); });
            });
    }

    private static String parseLn(String ln, String pat) {
        String[] poAry = ln.split(pat);
        String filLns = poAry[1].trim();
        return filLns;
    }

    private void process(Path dir, String ptrnStr, boolean isDeltaPO) {
        Deque<String> failedFileLst = new LinkedList<String>();
        Deque<String> successFileLst = new LinkedList<String>();
        Deque<String> fileLst = new LinkedList<String>();
        Deque<String> invDatafailedFileLst = new LinkedList<String>();

        try {
            Pattern pattern = Pattern.compile(ptrnStr);
            boolean hasPtrn = pattern.matcher(dir.toString()).find();
            if (isDeltaPO) {
                if (hasPtrn) {
                    Files.walk(dir).filter((p) -> (pattern.matcher(p.toString()).find()) &&
                                           Files.isRegularFile(p)).forEach((r) -> fileLst.add(r.toString()));
                } else {
                    //changing from lvlDepth 2 to 3 and not inside ARCHIVE. This is to accommodate "VARIABLE DATA", "DATA UPLOAD" cases.
                    Files.walk(dir, 2,
                               FileVisitOption.FOLLOW_LINKS).filter((p) -> (Files.isRegularFile(p))).forEach((r) -> fileLst.add(r.toString()));
                    //Files.walk(dir, 3, FileVisitOption.FOLLOW_LINKS).filter((p) -> (!pattern.matcher(p.toString()).find()) && Files.isRegularFile(p)).forEach((r) -> fileLst.add(r.toString()));
                }
                
                fileLst.forEach((p) -> { try {
                        boolean isChkedIn = sendToWCC(new File(p.toString()), hasPtrn);
                        if (isChkedIn) {
                            successFileLst.add(p.toString());
                        } else {
                            invDatafailedFileLst.add(p.toString());
                        }
                    } catch (Exception e) {
                        failedFileLst.add(p.toString());
                        logger.error("fail on: " + p.toString() + " moving on...");
                        logger.error(e.getMessage());
                    } });
            }
            else {
                sendToWCC(new File(dir.toString()), hasPtrn);
                successFileLst.add(dir.toString());
            }
            
            if (successFileLst != null && successFileLst.size() > 0) {
                writeFileListToQueue(dir, successFileLst, 0);
            }
            if (failedFileLst != null && failedFileLst.size() > 0) {
                writeFileListToQueue(dir, failedFileLst, 1);
            }
            if (invDatafailedFileLst != null && invDatafailedFileLst.size() > 0) {
                writeFileListToQueue(dir, invDatafailedFileLst, 2);
            }
        } catch (Exception e) {
            failedFileLst.add(dir.toString());
            writeFileListToQueue(dir, failedFileLst, false);
            logger.error("fail on: " + dir.toString() + " moving on...");
            logger.error(e.getMessage());
            e.printStackTrace();
        }
    }

    private void writeFileListToQueue(Path dir, Deque<String> fileLst, boolean successFiles) {
        if (fileLst != null && fileLst.size() > 0) {
            String files = fileLst.stream().collect(Collectors.joining(", ")).toString();
            files = "PO:" + getPONum(dir.toString()) + " files = " + files;
            if (successFiles) {
                successWriter.log(files);
            } else {
                failureWriter.log(files);
            }
        } else {
            System.out.println("Empty PO directory for PO: " + dir.getFileName());
        }
    }
    
    private void writeFileListToQueue(Path dir, Deque<String> fileLst, int logTyp) {
        if (fileLst != null && fileLst.size() > 0) {
            String files = fileLst.stream().collect(Collectors.joining(", ")).toString();
            files = "PO:" + dir.getFileName() + " files = " + files;
            switch (logTyp) {
                //supplying directory upto month
            case 0:
                successWriter.log(files);
                break;
            case 1:
                failureWriter.log(files);
                break;
            case 2:
                dataFailureWriter.log(files);
                break;
            default:
                //If logTyp doesn't match, don't log
                break;
            }
        } else {
            System.out.println("Empty PO directory for PO: " + dir.getFileName());
        }
    }
    
    private String getPONum(String fileName) {
        String po = null;
        Pattern ptrn = Pattern.compile(".?\\d{6,}.?");
        
        Matcher m = ptrn.matcher(fileName);
        if (m.find()) {
            po = m.group().replace("/", "");
        }
        
        return po;
    }

    public static IdcClient getRIDCClient(String instance, IdcContext idcCtx, int connTimeout,
                                          int connSize) throws IdcClientException {
        IdcClient idcClient;

        // create the manager
        if (mIdcClientMgr == null) {
            mIdcClientMgr = new IdcClientManager();
        }

        idcClient = mIdcClientMgr.getClient(instance);

        if (idcClient != null && !isClientValid(idcClient, idcCtx)) {
            logger.info("Invalid idc connection, removing IdcClient.");
            mIdcClientMgr.removeClient(instance);
            idcClient = null;
        }

        if (idcClient == null) {
            logger.info("Initiating new IdcClient.");
            idcClient = mIdcClientMgr.createClient(instance);
            idcClient.getConfig().setSocketTimeout(connTimeout);
            idcClient.getConfig().setConnectionSize(connSize);
            if (idcClient != null) {
                mIdcClientMgr.addClient(instance, idcClient);
            } else {
                throw new IdcClientException("Failed to open idc connection with WCC : " + instance);
            }

        }

        return idcClient;
    }

    public static boolean isClientValid(IdcClient idcClient, IdcContext idcCtx) {
        boolean clientValid = true;
        ServiceResponse response = null;
        DataBinder responseBinder = null;

        try {
            DataBinder binder = idcClient.createBinder();
            binder.putLocal("IdcService", "PING_SERVER");
            response = idcClient.sendRequest(idcCtx, binder);
            //Convert the response to a dataBinder
            responseBinder = response.getResponseAsBinder();
            String statusCdStr = responseBinder.getLocal("StatusCode");
            int statusCd = 0;
            if (statusCdStr != null && statusCdStr.trim().length() > 0) {
                statusCd = Integer.parseInt(statusCdStr);
            }
            String statusMsg = responseBinder.getLocal("StatusMessage");
            logger.info("Status code from PING call: " + statusCdStr + ", Status message: " + statusMsg);

            if (statusCd < 0) {
                clientValid = false;
                logger.info("Invalid IdcClient connection:  statusMsg/statusCd: " + statusMsg + "/" + statusCd);
            }
        } catch (IdcClientException e) {
            clientValid = false;
            logger.info("Invalid IdcClient connection: IdcClientException: ", e);
        }

        return clientValid;
    }

    private void deleteFile(File fileEntry) {
        logger.info("delete NOT implemented yet");
        //		fileEntry.delete();

    }

    public String contentExistSrch(String queryTxt) {

        ServiceResponse srchSrvResp = null;
        boolean srchSuccess = false;
        String docNam = null;

        try {
            IdcContext userContext = new IdcContext("weblogic", "welcome1");
            IdcClient idcClient = getRIDCClient(SERVER, userContext, 10000, idcConnPoolSz);
            DataBinder srchBinder = idcClient.createBinder();
            srchBinder.putLocal("IdcService", "GET_SEARCH_RESULTS");
            srchBinder.putLocal("QueryText", queryTxt);
            srchBinder.putLocal("ResultCount", "1");

            srchSrvResp = idcClient.sendRequest(userContext, srchBinder);
            DataBinder srchSrvRespBinder = srchSrvResp.getResponseAsBinder();
            //DataObject srchDataObject = srchSrvRespBinder.getLocalData();
            DataResultSet drsSearchResults = srchSrvRespBinder.getResultSet("SearchResults");
            List<DataObject> srchRsltLst = drsSearchResults.getRows();

            // loop over the results
            //            for (DataObject dataObject : srchRsltLst) {
            //                String docTtl = dataObject.get ("dDocTitle");
            //                String docAuth = dataObject.get ("dDocAuthor");
            //            }

            if (srchRsltLst.size() > 0) {
                docNam = srchRsltLst.get(0).get("dDocName");
            }

            logger.info("Search query matched dDocName = " + docNam);

        } catch (IdcClientException idcce) {
            //            logger.error("IDC Client Exception occurred. Unable to checkout file. Message: " +
            //                               idcce.getMessage() + ", Stack trace: ");
            //            idcce.printStackTrace();
        } catch (Exception e) {
            //            logger.error("Exception occurred. Unable to check out file. Message: " + e.getMessage() +
            //                               ", Stack trace: ");
            //            e.printStackTrace();
        } finally {
            if (srchSrvResp != null) {
                srchSrvResp.close();
            }
        }

        return docNam;
    }

    public boolean checkOutFile(String dDocName) {

        ServiceResponse chkoutSrvResp = null;
        boolean chkoutSuccess = false;

        try {
            IdcContext userContext = new IdcContext("weblogic", "welcome1");
            IdcClient idcClient = getRIDCClient(SERVER, userContext, 10000, idcConnPoolSz);
            DataBinder chkoutBinder = idcClient.createBinder();
            chkoutBinder.putLocal("IdcService", "CHECKOUT_BY_NAME");
            chkoutBinder.putLocal("dDocName", dDocName);

            chkoutSrvResp = idcClient.sendRequest(userContext, chkoutBinder);
            DataBinder chkoutSrvRespBinder = chkoutSrvResp.getResponseAsBinder();
            DataObject chkoutDataObject = chkoutSrvRespBinder.getLocalData();
            chkoutSuccess = true;
            logger.info(chkoutSrvRespBinder.getLocal("dDocName") + " checked out successfully");

        } catch (IdcClientException idcce) {
            //            logger.error("IDC Client Exception occurred. Unable to checkout file. Message: " +
            //                               idcce.getMessage() + ", Stack trace: ");
            //            idcce.printStackTrace();
        } catch (Exception e) {
            //            logger.error("Exception occurred. Unable to check out file. Message: " + e.getMessage() +
            //                               ", Stack trace: ");
            //            e.printStackTrace();
        } finally {
            if (chkoutSrvResp != null) {
                chkoutSrvResp.close();
            }
        }

        return chkoutSuccess;
    }

    public boolean fileExists(String dDocName) {

        ServiceResponse docInfoSrvResp = null;
        boolean docExists = false;

        try {
            IdcContext userContext = new IdcContext("weblogic", "welcome1");
            IdcClient idcClient = getRIDCClient(SERVER, userContext, 10000, idcConnPoolSz);
            DataBinder docInfoBinder = idcClient.createBinder();
            docInfoBinder.putLocal("IdcService", "DOC_INFO_BY_NAME");
            docInfoBinder.putLocal("dDocName", dDocName);

            docInfoSrvResp = idcClient.sendRequest(userContext, docInfoBinder);
            DataBinder docInfoSrvRespBinder = docInfoSrvResp.getResponseAsBinder();
            //DataObject docInfoDataObject = docInfoSrvRespBinder.getLocalData();
            String docUrl = docInfoSrvRespBinder.getLocal("DocUrl");
            if (docUrl != null && docUrl.trim().length() > 0) {
                docExists = true;
            }
            logger.info(docUrl + " exists in the system.");
        } catch (IdcClientException idcce) {
            //            logger.error("IDC Client Exception occurred. Unable to checkout file. Message: " +
            //                               idcce.getMessage() + ", Stack trace: ");
            //            idcce.printStackTrace();
        } catch (Exception e) {
            //            logger.error("Exception occurred. Unable to check out file. Message: " + e.getMessage() +
            //                               ", Stack trace: ");
            //            e.printStackTrace();
        } finally {
            if (docInfoSrvResp != null) {
                docInfoSrvResp.close();
            }
        }

        return docExists;
    }

    private boolean sendToWCC(File file, boolean hasPtrn) throws IdcClientException, IOException, SQLException {
        boolean isChkedIn = false;
        logger.info("\n");
        logger.info("Working on: " + file.getName());

        // build a client that will communicate using the intradoc protocol
        IdcContext userContext = new IdcContext("weblogic", "welcome1");
        IdcClient idcClient = getRIDCClient(SERVER, userContext, 10000, idcConnPoolSz);

        try (FileInputStream fs = new FileInputStream(file)) {

            String contentType = Files.probeContentType(file.toPath());

            Document doc = new Document();
            doc.populateFileAttributes(file.getName(), file.getPath(), hasPtrn, null);
            doc.setLastModifiedDate(file.lastModified());

            logger.info(doc.toString());

            String secGrp = doc.getSecurityGroup();
            String docTyp = doc.getDocType();
            String idxDocTyp = doc.getIndexDocumentType();
            String docAuth = "weblogic";
            String newFilNam = doc.getNewFileName();
            String slsOrd = doc.getSalesOrder();
            String inDt = DataObjectEncodingUtils.encodeDate(new Date(file.lastModified()));
            //String inDt = doc.getFormattedDate();
            String docTitle = doc.getTitle();
            String idxVerNum = doc.getIndexVerNum();
            String imprtYr = doc.getImportYear();
            String filExtn = doc.getFileExt();

            //String mtchdDocNam = getSearchResults("dDocTitle <matches> `" + docTitle + "`");

            DataBinder binder = idcClient.createBinder();

            //        if (mtchdDocNam != null && mtchdDocNam.trim().length() > 0) {
            //            checkOutFile(mtchdDocNam);
            //if (cntntExistsChkMap.containsKey(docTitle)) {
            String queryTxt = "dDocTitle <matches> `" + docTitle + "` <AND> dExtension <matches> `" + filExtn + "`";
            String mtchdDocNam = contentExistSrch(queryTxt);
            if (mtchdDocNam != null && mtchdDocNam.trim().length() > 0) {
                checkOutFile(mtchdDocNam);
                binder.putLocal("dDocName", mtchdDocNam);
            }
            if (idxVerNum != null && idxVerNum.trim().length() > 0) {
                int revLbl = Integer.valueOf(idxVerNum).intValue();
                binder.putLocal("dRevLabel", String.valueOf(revLbl));
                binder.putLocal("xBICIndexVersionNumber", idxVerNum);
            }

            binder.putLocal("dSecurityGroup", secGrp);
            binder.putLocal("dDocAccount", "");
            binder.putLocal("dDocType", docTyp);
            binder.putLocal("xIndexDocumentType", idxDocTyp);
            binder.putLocal("dDocAuthor", docAuth);
            binder.putLocal("dDocTitle", docTitle);
            binder.putLocal("xNewFileName", newFilNam);
            binder.putLocal("dInDate", inDt);
            binder.putLocal("xSalesOrder", slsOrd);
            binder.putLocal("xImportYear", imprtYr);
            if (BIC_SITE != null && BIC_SITE.trim().length() > 0) {
                binder.putLocal("xSite", BIC_SITE);
            }
            TransferFile tf = new TransferFile(fs, file.getName(), fs.available(), contentType);
            binder.addFile("primaryFile", tf);
            binder.putLocal("IdcService", "CHECKIN_UNIVERSAL");

            ServiceResponse srvResp = null;

            try {
                srvResp = idcClient.sendRequest(userContext, binder);
                //        logger.info( files +":" +serviceResponse.getResponseAsString() );
                DataBinder dataBinderResp = srvResp.getResponseAsBinder();
                String docNam = dataBinderResp.getLocalData().get("dDocName");

                logger.info(files + " dDocName is: " + docNam);
                files++;
            } finally {
                if (srvResp != null) {
                    srvResp.close();
                }
            }
            isChkedIn = true;
        }

        return isChkedIn;

    }

    private static void appendStringToFile(Path file, String s) throws IOException {
        try (BufferedWriter out = Files.newBufferedWriter(file, StandardCharsets.UTF_8, StandardOpenOption.APPEND)) {
            out.append(s);
            out.newLine();
        }
    }
}
