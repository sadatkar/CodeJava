package com.bic.migration;

import java.text.SimpleDateFormat;

import java.util.Date;

public class ImageDoc {


    private String title;
    private String doctype;
    private String securitygroup;
    private String fileName;
    private Long lastModifiedDate;
    private String formattedDate;
    private String indexDocumentType;
    private String newFileName;
    private String fileExt;
    private String docName;
    private String productId;

    private void setProductId(String productId) {
        this.productId = productId;
    }


    public void setFileName(String filename) {


        setDocName(filename);
        setTitle(filename);

        setDocType("ProductImage");
        setNewFileName(filename);
        setFileExt(filename.substring(filename.lastIndexOf(".") + 1));
        setSecurityGroup("CustomerService"); //this is TEMPORARY until security model finished
        setProductId(filename);
    }


    public String getSecurityGroup() {
        // TODO Auto-generated method stub
        return securitygroup;
    }

    public String getDocType() {
        // TODO Auto-generated method stub
        return doctype;
    }

    public String getIndexDocumentType() {
        // TODO Auto-generated method stub
        return indexDocumentType;
    }

    public void setLastModifiedDate(Long lastModifiedDate) {
        this.lastModifiedDate = lastModifiedDate;
        this.formattedDate = setFormattedDate(lastModifiedDate);

    }

    private String setFormattedDate(long in) {
        Date d = new Date(in);
        SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yy hh:mm a");
        return sdf.format(d);
    }

    public String getFormattedDate() {
        return formattedDate;
    }


    private void setSecurityGroup(String in) {
        this.securitygroup = in;

    }

    private void setFileExt(String in) {
        this.fileExt = in;

    }

    private void setNewFileName(String in) {
        this.newFileName = in;

    }

    private void setDocType(String in) {
        this.doctype = in;
    }

    private void setTitle(String in) {
        this.title = in;

    }

    private void setDocName(String in) {
        this.docName = in;
    }

    public String getTitle() {
        // TODO Auto-generated method stub
        return title;
    }

    public String getNewFileName() {
        // TODO Auto-generated method stub
        return newFileName;
    }

    public String getLastModifiedDate() {
        // TODO Auto-generated method stub
        return lastModifiedDate.toString();
    }


    public String getProductId() {
        // TODO Auto-generated method stub
        return productId;
    }

    @Override
    public String toString() {

        StringBuilder sb = new StringBuilder();
        String spacer = "\r";


        sb.append("doctype:" + getDocType());
        sb.append(spacer);

        sb.append("formattedDate:" + getFormattedDate());
        sb.append(spacer);

        sb.append("newFileName:" + getNewFileName());
        sb.append(spacer);
        sb.append("ProductId:" + getProductId());
        sb.append(spacer);
        sb.append("SecurityGroup:" + getSecurityGroup());
        sb.append(spacer);
        sb.append("Title:" + getTitle());


        return sb.toString();
    }


}
