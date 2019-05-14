package com.bic.migration;

import java.io.IOException;
import java.io.InputStream;

import java.util.Iterator;

import org.apache.commons.lang.StringUtils;

import oracle.stellent.ridc.IdcClient;
import oracle.stellent.ridc.IdcClientException;
import oracle.stellent.ridc.IdcClientManager;
import oracle.stellent.ridc.IdcContext;
import oracle.stellent.ridc.model.DataBinder;
import oracle.stellent.ridc.model.TransferFile;
import oracle.stellent.ridc.protocol.ServiceResponse;

public class RidcClient {
    static String SERVER = "idc://soabpm-vm.site:4444";
    IdcClient idcLient = null;

    /**
     * @param args
     */
    public static void main(String[] args) {
        // TODO Auto-generated method stub

    }

    public RidcClient() throws IdcClientException {
        IdcClientManager manager = new IdcClientManager();
        // build a client that will communicate using the intradoc protocol
        IdcClient idcClient = manager.createClient(SERVER);
        idcClient.getConfig().setSocketTimeout(5000); // 30 seconds
        idcClient.getConfig().setConnectionSize(20); // 20 connections
        IdcContext userContext = new IdcContext("weblogic", "welcome1");
    }

    public void checkIn(IdcClient client, String username, Document doc,
                        InputStream attachment,
                        String contentType) throws IdcClientException,
                                                   IOException {
        DataBinder binder = client.createBinder();
        // populate the binder with the parameters
        binder.putLocal("IdcService", "CHECKIN_UNIVERSAL");

        binder.putLocal("dSecurityGroup", doc.getSecurityGroup());
        binder.putLocal("dDocAccount", "");
        binder.putLocal("dDocName", doc.getDocumentId());
        binder.putLocal("dDocType", doc.getDocType());
        binder.putLocal("dDocAuthor", "TESTUSER");
        binder.putLocal("dDocTitle", doc.getTitle());
        //		binder.putLocal("dCollectionID", doc.getContentId());
        binder.putLocal("isFinished", "true");
        TransferFile tf =
            new TransferFile(attachment, doc.getTitle(), attachment.available(),
                             contentType);
        binder.addFile("primaryFile", tf);

        ServiceResponse serviceResponse =
            client.sendRequest(new IdcContext(username), binder);
        DataBinder dataBinderResp = serviceResponse.getResponseAsBinder();
    }


}
