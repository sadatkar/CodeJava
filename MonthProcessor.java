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
import java.util.Date;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.RecursiveAction;
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

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class MonthProcessor extends RecursiveAction {
    @SuppressWarnings("compatibility:-3557088717744207593")
    private static final long serialVersionUID = 1L;
    transient Logger logger = LogManager.getLogger(MonthProcessor.class);
    transient IdcClientManager mIdcClientMgr;
    String idcServer = "idc://clwslwcc01d:4444";
    int idcConnPoolSz = 20;
    
    private String revPtrnStr = null;
    private String fltrPtrnStr = null;
    private String poPtrnStr = null;
    private Path path = null;
    static int files = 0;
    Set<String> mnthSet = null;
    
    FileWriter successWriter = null;
    FileWriter failureWriter = null;
    FileWriter dataFailureWriter = null;

    public MonthProcessor(Path path, String revPtrnStr, String fltrPtrnStr, String poPtrnStr, String idcServer, int idcConnPoolSz, Set<String> mnthSet, FileWriter successWriter, FileWriter failureWriter, FileWriter dataFailureWriter) {
        this.revPtrnStr = revPtrnStr;
        this.fltrPtrnStr = fltrPtrnStr;
        this.poPtrnStr = poPtrnStr;
        this.path = path;
        this.mnthSet = mnthSet;
        this.idcServer = idcServer;
        this.idcConnPoolSz = idcConnPoolSz;
        this.successWriter = successWriter;
        this.failureWriter = failureWriter;
        this.dataFailureWriter = dataFailureWriter;
        //this.logInstanceDetails();
    }

    void processParallel() throws IOException {
        List<MonthProcessor> tasks = new ArrayList<MonthProcessor>();
        
        if (mnthSet != null && mnthSet.size() > 0 && path == null) {
            for (String mnthStr : mnthSet) {
                System.out.println("Processing MONTH: " + mnthStr + " .....");
                Path mnthPath = Paths.get(mnthStr);
                MonthProcessor mnthPrcssr =
                    new MonthProcessor(mnthPath, revPtrnStr, fltrPtrnStr, poPtrnStr, idcServer, idcConnPoolSz, null, successWriter,
                                       failureWriter, dataFailureWriter);
                mnthPrcssr.fork();
                tasks.add(mnthPrcssr);
            }
            files = countFilePrcssedByTasks(tasks);
        }
        //else {
        if (mnthSet == null && path != null) {
            System.out.println("Processing PATH: " + path.toString() + " .....");
            
            Files.walk(path, 1,
                       FileVisitOption.FOLLOW_LINKS).filter(( p) -> (!p.toString().equalsIgnoreCase(path.toString()))).parallel().forEach(( p) -> {
                    Map<String, String> prcsdTtlDocNamMap = new HashMap<String, String>();
                    //process(p, revPtrnStr, prcsdTtlDocNamMap);
                    if (revPtrnStr != null && revPtrnStr.trim().length() > 0) process(p, revPtrnStr, prcsdTtlDocNamMap);
                    process(p, null, prcsdTtlDocNamMap);
                });
        }
    }

    private void logInstanceDetails() {
        StringBuffer sb = new StringBuffer();
        sb.append("this.revPtrnStr:");
        sb.append(this.revPtrnStr);
        sb.append(",");
        sb.append("this.fltrPtrnStr:");
        sb.append(this.fltrPtrnStr);
        sb.append(",");
        sb.append("this.path:");
        sb.append(this.path);
        sb.append(",");
        if (this.mnthSet != null && this.mnthSet.size() > 0) {
            sb.append("this.mnthSet:");
            sb.append(this.mnthSet.toString());
            sb.append(",");
        }
        sb.append("this.idcServer:");
        sb.append(this.idcServer);
        sb.append(",");
        sb.append("this.idcConnPoolSz:");
        sb.append(this.idcConnPoolSz);
        sb.append(",");
        sb.append("this.successWriter:");
        sb.append(this.successWriter.hashCode());
        sb.append(",");
        sb.append("this.failureWriter:");
        sb.append(this.failureWriter.hashCode());
        sb.append(",");
        sb.append("this.dataFailureWriter:");
        sb.append(this.dataFailureWriter.hashCode());
        
        System.out.println("MonthProcessor fields: " + sb.toString() + " .....\n");
    }
    
    public int countFilePrcssedByTasks(List<MonthProcessor> tasks)
   {
      int ttlFilePrcssd = 0;
      for (MonthProcessor prcssr : tasks)
      {
         prcssr.join();
         ttlFilePrcssd = ttlFilePrcssd + prcssr.files;
      }
      
      return ttlFilePrcssd;
   }

    void process(Path dir, String ptrnStr, Map<String, String> prcsdTtlDocNamMap) {


        //If filter pattern is supplied, use it to
        // filter PO. Sample pattern below.
        //Pattern patNum = Pattern.compile("\\d{9,}");
        //Pattern patMixed = Pattern.compile("[A-Za-z]\\d{5,}");

        if (fltrPtrnStr != null && fltrPtrnStr.trim().length() > 0) {
            Pattern fltrPat = Pattern.compile(fltrPtrnStr);
            Matcher m = fltrPat.matcher(dir.toString());

            if (m.find()) {
                processFiltered(dir, ptrnStr, prcsdTtlDocNamMap);
            }
        } else {
            processFiltered(dir, ptrnStr, prcsdTtlDocNamMap);
        }
    }

    void processFiltered(Path dir, String ptrnStr, Map<String, String> prcsdTtlDocNamMap) {
        Deque<String> fileLst = new LinkedList<String>();
        Deque<String> failedFileLst = new LinkedList<String>();
        Deque<String> invDatafailedFileLst = new LinkedList<String>();
        Deque<String> successFileLst = new LinkedList<String>();
        //Map<String, String> prcsdTtlDocNamMap = new HashMap<String, String>();
        boolean hasPtrn = (ptrnStr != null && ptrnStr.trim().length() > 0);

        try {
            if (hasPtrn) {
                Pattern pattern = Pattern.compile(ptrnStr);
                Files.walk(dir).filter(( p) -> (pattern.matcher(p.toString()).find()) &&
                                       Files.isRegularFile(p)).forEach(( r) -> fileLst.add(r.toString()));
            } else {
                //Pattern pattern = Pattern.compile("ARCHIVE");
                //changing from lvlDepth 2 to 3 and not inside ARCHIVE. This is to accommodate "VARIABLE DATA", "DATA UPLOAD" cases.
                Files.walk(dir, 2,
                           FileVisitOption.FOLLOW_LINKS).filter(( p) -> (Files.isRegularFile(p))).forEach(( r) -> fileLst.add(r.toString()));
                //Files.walk(dir, 3, FileVisitOption.FOLLOW_LINKS).filter((p) -> (!pattern.matcher(p.toString()).find()) && Files.isRegularFile(p)).forEach((r) -> fileLst.add(r.toString()));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        fileLst.forEach(( p) -> { try {
                boolean isChkedIn = sendToWCC(new File(p.toString()), prcsdTtlDocNamMap, hasPtrn);
                if (isChkedIn) {
                    successFileLst.add(p.toString());
                } else {
                    invDatafailedFileLst.add(p.toString());
                }
            } catch (Exception e) {
                failedFileLst.add(p.toString());
                logger.error("fail on: " + p.toString() + " moving on...");
                logger.error(e.getMessage());
                e.printStackTrace();
            } });

        if (successFileLst != null && successFileLst.size() > 0) {
            writeFileListToQueue(dir, successFileLst, 0);
        }
        if (failedFileLst != null && failedFileLst.size() > 0) {
            writeFileListToQueue(dir, failedFileLst, 1);
        }
        if (invDatafailedFileLst != null && invDatafailedFileLst.size() > 0) {
            writeFileListToQueue(dir, invDatafailedFileLst, 2);
        }
    }

    void writeFileListToQueue(Path dir, Deque<String> fileLst, int logTyp) {
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

    public IdcClient getRIDCClient(String instance, IdcContext idcCtx, int connTimeout,
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

    public boolean isClientValid(IdcClient idcClient, IdcContext idcCtx) {
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
                logger.info("Invalid IdcClient connection:  statusMsg/statusCd: " + statusMsg + "/" +
                                         statusCd);
            }
        } catch (IdcClientException e) {
            clientValid = false;
            logger.info("Invalid IdcClient connection: IdcClientException: ", e);
        }

        return clientValid;
    }

//    void deleteFile(File fileEntry) {
//        logger.info("delete NOT implemented yet");
//        fileEntry.delete();
//    }

    public String fileExistsSrch(String queryTxt, String docTitle, String docExtn) {

        ServiceResponse srchSrvResp = null;
        boolean filExist = false;
        String docNam = null;

        try {
            IdcContext userContext = new IdcContext("weblogic", "welcome1");
            IdcClient idcClient = getRIDCClient(idcServer, userContext, 10000, idcConnPoolSz);
            DataBinder srchBinder = idcClient.createBinder();
            srchBinder.putLocal("IdcService", "GET_SEARCH_RESULTS");
            srchBinder.putLocal("QueryText", queryTxt);
            srchBinder.putLocal("ResultCount", "20");

            srchSrvResp = idcClient.sendRequest(userContext, srchBinder);
            DataBinder srchSrvRespBinder = srchSrvResp.getResponseAsBinder();
            //DataObject srchDataObject = srchSrvRespBinder.getLocalData();
            DataResultSet drsSearchResults = srchSrvRespBinder.getResultSet("SearchResults");
            List<DataObject> srchRsltLst = drsSearchResults.getRows();

            // loop over the results
            for (DataObject dataObject : drsSearchResults.getRows()) {
                String ttl = dataObject.get("dDocTitle");
                String extn = dataObject.get("dExtension");

                if ((ttl != null && ttl.trim().length() > 0 && ttl.equalsIgnoreCase(docTitle)) &&
                    (extn != null && extn.trim().length() > 0 && extn.equalsIgnoreCase(docExtn))) {
                    docNam = dataObject.get("dDocName");
                    break;
                }
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
            IdcClient idcClient = getRIDCClient(idcServer, userContext, 10000, idcConnPoolSz);
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
            IdcClient idcClient = getRIDCClient(idcServer, userContext, 10000, idcConnPoolSz);
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

    boolean sendToWCC(File file, Map<String, String> cntntExistsChkMap, boolean hasPtrn) throws IdcClientException,
                                                                                                IOException,
                                                                                                SQLException {
        boolean isChkedIn = false;
        logger.info("\n");
        logger.info("Working on: " + file.getName());

        // build a client that will communicate using the intradoc protocol
        IdcContext userContext = new IdcContext("weblogic", "welcome1");
        IdcClient idcClient = getRIDCClient(idcServer, userContext, 10000, idcConnPoolSz);

        try (FileInputStream fs = new FileInputStream(file)) {

            String contentType = Files.probeContentType(file.toPath());

            Document doc = new Document();
            doc.populateFileAttributes(file.getName(), file.getPath(), hasPtrn, poPtrnStr);
            doc.setLastModifiedDate(file.lastModified());

            logger.info(doc.toString());

            String secGrp = doc.getSecurityGroup();
            String docTyp = doc.getDocType();
            String idxDocTyp = doc.getIndexDocumentType();
            String docAuth = "weblogic";
            String newFilNam = doc.getNewFileName();
            String slsOrd = doc.getSalesOrder();
            String fldr = doc.getFldr();
            String inDt = DataObjectEncodingUtils.encodeDate(new Date(file.lastModified()));
            //String inDt = doc.getFormattedDate();
            String docTitle = doc.getTitle();
            String idxVerNum = doc.getIndexVerNum();
            String imprtYr = doc.getImportYear();

            //Pattern patNum = Pattern.compile("\\d{9,}");
            //Pattern patMixed = Pattern.compile("[A-Za-z]\\d{5,}");
            
            boolean isSlsOrdOK = false;

            if (slsOrd != null && slsOrd.trim().length() > 0) {
                if (fltrPtrnStr != null && fltrPtrnStr.trim().length() > 0) {
                    Pattern fltrPat = Pattern.compile(fltrPtrnStr);
                    Matcher m = fltrPat.matcher(slsOrd.toString());

                    if (m.find()) {
                        isSlsOrdOK = true;
                    }
                } else {
                    isSlsOrdOK = true;
                }
            }

            if (isSlsOrdOK) {
                DataBinder binder = idcClient.createBinder();

                //        if (mtchdDocNam != null && mtchdDocNam.trim().length() > 0) {
                //            checkOutFile(mtchdDocNam);
                //if (cntntExistsChkMap.containsKey(docTitle)) {
                String cntntExistsKey = slsOrd + "#" + idxDocTyp + "#" + newFilNam;
                if (cntntExistsChkMap.containsKey(cntntExistsKey)) {
                    String existDocNam = cntntExistsChkMap.get(cntntExistsKey);
                    checkOutFile(existDocNam);
                    binder.putLocal("dDocName", existDocNam);
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
                binder.putLocal("xFolderName", fldr);
                binder.putLocal("xImportYear", imprtYr);
                TransferFile tf = new TransferFile(fs, file.getName(), fs.available(), contentType);
                binder.addFile("primaryFile", tf);
                binder.putLocal("IdcService", "CHECKIN_UNIVERSAL");

                ServiceResponse srvResp = null;

                try {
                    srvResp = idcClient.sendRequest(userContext, binder);
                    //logger.info( files +":" +srvResp.getResponseAsString() );
                    DataBinder dataBinderResp = srvResp.getResponseAsBinder();
                    String docNam = dataBinderResp.getLocalData().get("dDocName");

                    logger.info(" dDocName is: " + docNam);
                    //cntntExistsChkMap.put(docTitle, docNam);
                    cntntExistsChkMap.put(cntntExistsKey, docNam);

                    files++;
                } 
                catch (Exception e) {
                    e.printStackTrace();
                }
                finally {
                    if (srvResp != null) {
                        srvResp.close();
                    }
                }
                isChkedIn = true;
            }
        }

        return isChkedIn;
    }

    static void appendStringToFile(Path file, String s) throws IOException {
        try (BufferedWriter out = Files.newBufferedWriter(file, StandardCharsets.UTF_8, StandardOpenOption.APPEND)) {
            out.append(s);
            out.newLine();
        }
    }

    @Override
    protected void compute() {
        try {
            processParallel();
        } catch (IOException e) {
        }
    }
}
