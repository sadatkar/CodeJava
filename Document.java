package com.bic.migration;

import java.io.File;

import java.text.SimpleDateFormat;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class Document {
    private static Logger logger = LogManager.getLogger(Document.class);

    private String contentId;
    private String documentId;
    private String docName;
    private String owner;
    private String title;
    private String doctype;
    private String securitygroup;
    private String filePath;
    private String salesOrder;
    private Long lastModifiedDate;
    private String formattedDate;
    private String indexDocumentType;
    private String indexVerNum;
    private String newFileName;
    private String fileExt;
    private String fileNam;
    private String lineNum;
    private String importYear;
    private String site;
    private String fldr;

    private static Map<String, String> idxDocTypMap = new HashMap<String, String>();
    private static List<Pattern> idxDocTypLst = null;

    public void populateFileAttributes(String filNam, String filPath, boolean hasPtrn, String mtchPatStr) {
        /**
    	 * parse filename into it's separate components and set in object - this is the thing that does the work
    	 */
        logger.info("Populating file attrs. filNam=" + filNam + " filPath=" + filPath + " hasPtrn=" + hasPtrn + " mtchPatStr=" + mtchPatStr);
        fileNam = filNam;
        filePath = filPath;

        if (idxDocTypLst == null || idxDocTypLst.size() == 0) {
            initIdxDocTypLst();
        }
        if (idxDocTypMap == null || idxDocTypMap.size() == 0) {
            initIdxDocTypMap();
        }

        String[] tmp = fileNameParse(filNam);
        String newFileNam = null;
        String newFileExt = filNam.substring(filNam.lastIndexOf(".") + 1);
        setFileExt(newFileExt);
        Pattern patNum = Pattern.compile("\\d{1,}");
        Pattern patChar = Pattern.compile("\\D{1,}");
        String lstElement = tmp != null && tmp.length > 0 ? tmp[tmp.length - 1].trim() : null;
        if (hasPtrn && patNum.matcher(lstElement).find() && !patChar.matcher(lstElement).find()) {
            int lstIdx= filNam.lastIndexOf("-");
            if (lstIdx > 0) {
                newFileNam = filNam.substring(0, filNam.lastIndexOf("-"));
                indexVerNum = lstElement.trim();                
            }
            else {
                newFileNam = filNam.substring(0, filNam.lastIndexOf("."));
            }

        } else {
            newFileNam = filNam.substring(0, filNam.lastIndexOf("."));
        }
        //String docNam = getFileNamePart(filename.replaceAll("\\s+", ""));
        setTitle(newFileNam);
        String po = getPOFrmPath(filPath, mtchPatStr);
        setFldr(getFolderFrmPath(filPath, po));
        setSalesOrder(po);
        setIndexDocumentType(getIdxDocTyp(filNam, filPath));
        setDocType("IndexDoc");
        //setNewFileName(newFileNam + "." + newFileExt);
        setNewFileName(filNam);
        setSecurityGroup("OMI");

        setImportYear(extractImportYear(filPath));
        logger.info(toString());
    }

    public void setFldr(String fldr) {
        this.fldr = fldr;
    }

    public String getFldr() {
        return fldr;
    }

    public String getImportYear() {
        return importYear;
    }

    private String extractImportYear(String filPath) {
        String imprtYr = null;
        Pattern patYr = Pattern.compile(".?\\d{4}.?");
        Matcher m = patYr.matcher(filPath);
        if (m.find()) {
            imprtYr = m.group().replaceAll("\\D", "");
        }

        return imprtYr;
    }

    public void setImportYear(String importYear) {
        this.importYear = importYear;
    }

    public String getIndexVerNum() {
        return indexVerNum;
    }

    public String getFilePath() {
        return filePath;
    }

    public String getLineNum() {
        return lineNum;
    }

    public void initIdxDocTypMap() {
        idxDocTypMap.put("CUSTART", "Customer Art");
        idxDocTypMap.put("PROOF", "Proof");
        idxDocTypMap.put("ARTFN", "Final Art");
        idxDocTypMap.put("CUSTPO", "Customer PO");
        idxDocTypMap.put("EMAIL", "Email");
        idxDocTypMap.put("MISC", "Misc");
        idxDocTypMap.put("CHECK", "Check");
        idxDocTypMap.put("REF", "Ref");
        idxDocTypMap.put("ADMIN", "Admin");
        idxDocTypMap.put("DATA UPLOAD", "Data Upload");
        idxDocTypMap.put("VARIABLE DATA", "Variable Data");
        //temp
        idxDocTypMap.put("FINALART", "Final Art");
        idxDocTypMap.put("PREVIOUS", "Previous");
    }

    public void initIdxDocTypLst() {
        idxDocTypLst = new ArrayList<Pattern>();
        idxDocTypLst.add(Pattern.compile("CUSTART"));
        idxDocTypLst.add(Pattern.compile("PROOF"));
        idxDocTypLst.add(Pattern.compile("ARTFN"));
        idxDocTypLst.add(Pattern.compile("CUSTPO"));
        idxDocTypLst.add(Pattern.compile("EMAIL"));
        idxDocTypLst.add(Pattern.compile("MISC"));
        idxDocTypLst.add(Pattern.compile("CHECK"));
        idxDocTypLst.add(Pattern.compile("REF"));
        idxDocTypLst.add(Pattern.compile("ADMIN"));
        idxDocTypLst.add(Pattern.compile("DATA UPLOAD"));
        idxDocTypLst.add(Pattern.compile("VARIABLE DATA"));
        //temp
        idxDocTypLst.add(Pattern.compile("FINALART"));
        idxDocTypLst.add(Pattern.compile("PREVIOUS"));
    }

    public String getIdxDocTyp(String filNam, String filPath) {
        String idxDocTyp = null;

        if (getFileExt().equalsIgnoreCase("MSG")) {
            idxDocTyp = idxDocTypMap.get("EMAIL");
        }

        if (idxDocTyp == null && idxDocTypLst != null) {
            for (Pattern ptrn : idxDocTypLst) {
                if (ptrn.matcher(filNam).find()) {
                    idxDocTyp = idxDocTypMap.get(ptrn.toString());
                    break;
                }
            }

            if (idxDocTyp == null || idxDocTyp.trim().length() == 0) {
                for (Pattern ptrn : idxDocTypLst) {
                    if (ptrn.matcher(filPath).find()) {
                        idxDocTyp = idxDocTypMap.get(ptrn.toString());
                        break;
                    }
                }
            }

            if (idxDocTyp != null && idxDocTyp.trim().length() > 0 && idxDocTyp.equals(idxDocTypMap.get("ADMIN"))) {
                idxDocTyp = idxDocTypMap.get("MISC");
            }

            //Default value incase nothing matched
            if (idxDocTyp == null || idxDocTyp.trim().length() == 0) {
                idxDocTyp = idxDocTypMap.get("MISC");
            }
        }

        return idxDocTyp;
    }

    public String getPONum(String filNam, String filPath) {
        String rtnPO = null;
        //        Pattern patNum = Pattern.compile("\\d{9,}");
        //        Pattern patChar = Pattern.compile("\\D{1,}");
        Pattern patMixed = Pattern.compile("\\d{9,}|[A-Za-z]\\d{5,}");
        //
        //        String[] filNamAry = fileNameParse(filNam);
        //        String po = filNamAry[0];
        //
        //        if (po != null && po.trim().length() > 0 && ((patNum.matcher(po).find() && !patChar.matcher(po).find()) || (patMixed.matcher(po).find()))) {
        //            rtnPO = po;
        //        } else {
        Matcher m = patMixed.matcher(filPath);
        if (m.find()) {
            rtnPO = m.group();
        }
        //        }

        return rtnPO;
    }

    public String getPOFrmPath(String filPath, String mtchPtrnStr) {
        String rtnPO = null;
        //        Pattern patNum = Pattern.compile("\\d{9,}");
        //        Pattern patChar = Pattern.compile("\\D{1,}");
        //        Pattern patMixed = Pattern.compile("\\d{9,}|[A-Za-z]\\d{5,}");

        Pattern mtchPtrn = Pattern.compile(mtchPtrnStr);
        Matcher m = mtchPtrn.matcher(filPath);
        if (m.find()) {
            rtnPO = m.group();
        }

        return rtnPO;
    }
    
    public String getFolderFrmPath(String filPath, String po) {
        String rtnFldr = null;
        logger.info("Path:" + filPath);
        logger.info("PO:" + po);
        logger.info("IdxPO:" + filPath.indexOf(po));
        logger.info("POLn:" + po.length());
        logger.info("LstIdx:" + filPath.lastIndexOf(File.separator));
        logger.info("Sep:" + File.separator);
        
        rtnFldr = filPath.substring(filPath.indexOf(po) + po.length(), filPath.lastIndexOf(File.separator));

        return rtnFldr;
    }

    public String getFileNamePart(String f) {
        String namePart = "";
        int i = f.lastIndexOf('.');
        if (i > 0 && i < f.length() - 1) {
            namePart = f.substring(0, i);
        }
        return namePart;
    }

    private void setLineNum(String lineNum) {
        this.lineNum = lineNum;
    }

    public String getFileExt() {
        return fileExt;
    }


    private void setFileExt(String fileExt) {
        this.fileExt = fileExt;
    }


    public String getNewFileName() {
        return newFileName;
    }


    private void setNewFileName(String newFileName) {
        this.newFileName = newFileName;
    }


    public String getIndexDocumentType() {
        return indexDocumentType;
    }


    public void setIndexDocumentType(String indexDocumentType) {
        this.indexDocumentType = indexDocumentType;
    }


    public String getLastModifiedDate() {
        return lastModifiedDate.toString();
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


    public String getSalesOrder() {
        return salesOrder;
    }

    private void setSalesOrder(String salesOrder) {
        this.salesOrder = salesOrder;
    }

    public String getDocName() {
        return docName;
    }

    private void setDocName(String docName) {
        this.docName = docName;
    }

    public String getDocumentId() {
        return documentId;
    }

    public String getOwner() {
        return owner;
    }

    private void setDocType(String doctype) {
        this.doctype = doctype;
    }

    public String getDocType() {
        return doctype;
    }

    private void setSecurityGroup(String securitygroup) {
        this.securitygroup = securitygroup;
    }

    public String getSecurityGroup() {
        return securitygroup;
    }


    private void setTitle(String title) {
        this.title = title;
    }

    public String getTitle() {
        return title;
    }


    public static String[] fileNameParse(String in) {
        //		100000397-ARTFN-1-SURFACE-CMYK-001.EPS
        return in.substring(0, in.lastIndexOf(".")).split("-");
        //	return in.split("-");

    }

    @Override
    public String toString() {

        StringBuilder sb = new StringBuilder();
        String spacer = " ";

        sb.append("docname:" + getDocName());
        sb.append(spacer);
        sb.append("doctype:" + getDocType());
        sb.append(spacer);
        sb.append("file ext:" + getFileExt());
        sb.append(spacer);
        sb.append("formattedDate:" + getFormattedDate());
        sb.append(spacer);
        sb.append("indexDocumentType:" + getIndexDocumentType());
        sb.append(spacer);
        sb.append("newFileName:" + getNewFileName());
        sb.append(spacer);
        sb.append("SalesOrder:" + getSalesOrder());
        sb.append(spacer);
        sb.append("Folder:" + getFldr());
        sb.append(spacer);
        sb.append("SecurityGroup:" + getSecurityGroup());
        sb.append(spacer);
        sb.append("Title:" + getTitle());
        sb.append(spacer);
        sb.append("Site:" + getSite());
        sb.append(spacer);
        sb.append("ImportYear:" + getImportYear());
        sb.append(spacer);
        sb.append("Path:" + getFilePath());


        return sb.toString();
    }

    public void setSite(String site) {
        this.site = site;
    }

    public String getSite() {
        return site;
    }
}
