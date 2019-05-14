package com.bic.migration;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import java.nio.file.Files;

import java.util.Arrays;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import oracle.stellent.ridc.IdcClient;
import oracle.stellent.ridc.IdcClientException;
import oracle.stellent.ridc.IdcClientManager;
import oracle.stellent.ridc.IdcContext;
import oracle.stellent.ridc.model.DataBinder;
import oracle.stellent.ridc.model.TransferFile;
import oracle.stellent.ridc.protocol.ServiceResponse;

public class WebImageImportPOC {

    private static final String SERVER = "idc://soabpm-vm:4444";
    //	private static final String SERVER = "idc://clwslwcc01d:4444";

    private static final String NORWOOD_BASEFOLDER =
        "/INSTALLS/BIC/XferFolder/fromNorwood_mysites/";
    private static final String BIC_BASEFOLDER =
        "/INSTALLS/BIC/XferFolder/FromBicSite_prod_images/800/800";


    private static Logger logger =
        LogManager.getLogger("ImageImportPOC.class");

    static int folders = 0;
    static int files = 0;

    /**
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {

        WebImageImportPOC poc = new WebImageImportPOC();
        //call for norwood images
        logger.info("BASEFOLDER IS: " + NORWOOD_BASEFOLDER);
        poc.listFilesForFolder(new File(NORWOOD_BASEFOLDER), true);

        //call for bic images
        logger.info("BASEFOLDER IS: " + BIC_BASEFOLDER);
        poc.listFilesForFolder(new File(BIC_BASEFOLDER), true);


        logger.info("Processed: folders :" + folders + " Files: " + files);
    }

    /**
     * @param sendtowcc
     * @throws Exception
     */


    public void listFilesForFolder(final File folder,
                                   boolean sendtowcc) throws Exception {
        for (final File fileEntry : folder.listFiles()) {
            if (fileEntry.isDirectory()) {
                folders++;
                listFilesForFolder(fileEntry, sendtowcc);
            } else {
                files++;
                if (sendtowcc) {
                    try {
                        if (fileEntry.getName().startsWith(".")) {
                            fileEntry.delete();
                        } else {
                            sendToWCC(fileEntry);
                            deleteFile(fileEntry); // only deleting if NO exception
                        }

                    } catch (IdcClientException e) {
                        System.err.println("fail on: " +
                                           fileEntry.getAbsoluteFile() +
                                           " moving on");
                        System.err.println(e.getMessage());
                    }
                } else {
                    logger.info("Listing files only: " + " " +
                                fileEntry.getAbsolutePath() + " " +
                                Arrays.toString(Document.fileNameParse(fileEntry.getName())));

                }
            }
        }
    }


    private void deleteFile(File fileEntry) {
        logger.info("delete NOT implemented yet");
        //		fileEntry.delete();

    }


    private void sendToWCC(File file) throws Exception, IdcClientException,
                                             IOException {
        logger.info("working on: " + file.getName());
        IdcClientManager manager = new IdcClientManager();
        IdcClient idcClient = manager.createClient(SERVER);

        idcClient.getConfig().setSocketTimeout(10000); // 10 seconds
        idcClient.getConfig().setConnectionSize(20); // 20 connections
        IdcContext userContext = new IdcContext("weblogic", "welcome1");


        FileInputStream fs = new FileInputStream(file);

        String contentType = Files.probeContentType(file.toPath());

        WebImageDoc doc = new WebImageDoc();
        doc.setFileName(file.getName());
        doc.setLastModifiedDate(file.lastModified());

        logger.info(doc.toString());

        DataBinder binder = idcClient.createBinder();
        binder.putLocal("IdcService", "CHECKIN_UNIVERSAL");
        binder.putLocal("dDocName", doc.getProductId());
        binder.putLocal("dSecurityGroup", doc.getSecurityGroup());
        binder.putLocal("dDocAccount", "");
        binder.putLocal("dDocType", doc.getDocType());

        binder.putLocal("dDocAuthor", "weblogic");
        binder.putLocal("dDocTitle", doc.getTitle());


        binder.putLocal("xSiSyncToSites", "true");
        binder.putLocal("xNewFileName", doc.getNewFileName());

        binder.putLocal("dInDate", doc.getLastModifiedDate());

        String prodid = doc.getProductId();
        binder.putLocal("xProductId", prodid);

        TransferFile tf =
            new TransferFile(fs, file.getName(), fs.available(), contentType);
        binder.addFile("primaryFile", tf);

        ServiceResponse serviceResponse =
            idcClient.sendRequest(userContext, binder);
        DataBinder dataBinderResp = serviceResponse.getResponseAsBinder();

    }


}
