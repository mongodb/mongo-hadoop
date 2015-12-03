package com.mongodb.spark.examples.enron;

/**
 * Created by bryan on 12/3/15.
 */
public class Message {

    private String id;
    private String body;
    private String mailbox;
    private String filename;
    private String X_cc;
    private String Subject;
    private String X_Folder;
    private String Content_Transfer_Encoding;
    private String X_bcc;
    private String To;
    private String X_Origin;
    private String X_FileName;
    private String X_From;
    private String Date;
    private String X_To;
    private String Message_ID;
    private String Content_Type;
    private String Mime_Version;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getBody() {
        return body;
    }

    public void setBody(String body) {
        this.body = body;
    }

    public String getMailbox() {
        return mailbox;
    }

    public void setMailbox(String mailbox) {
        this.mailbox = mailbox;
    }

    public String getFilename() {
        return filename;
    }

    public void setFilename(String filename) {
        this.filename = filename;
    }

    public String getX_cc() {
        return X_cc;
    }

    public void setX_cc(String x_cc) {
        X_cc = x_cc;
    }

    public String getSubject() {
        return Subject;
    }

    public void setSubject(String subject) {
        Subject = subject;
    }

    public String getX_Folder() {
        return X_Folder;
    }

    public void setX_Folder(String x_Folder) {
        X_Folder = x_Folder;
    }

    public String getContent_Transfer_Encoding() {
        return Content_Transfer_Encoding;
    }

    public void setContent_Transfer_Encoding(String content_Transfer_Encoding) {
        Content_Transfer_Encoding = content_Transfer_Encoding;
    }

    public String getX_bcc() {
        return X_bcc;
    }

    public void setX_bcc(String x_bcc) {
        X_bcc = x_bcc;
    }

    public String getTo() {
        return To;
    }

    public void setTo(String to) {
        To = to;
    }

    public String getX_Origin() {
        return X_Origin;
    }

    public void setX_Origin(String x_Origin) {
        X_Origin = x_Origin;
    }

    public String getX_FileName() {
        return X_FileName;
    }

    public void setX_FileName(String x_FileName) {
        X_FileName = x_FileName;
    }

    public String getX_From() {
        return X_From;
    }

    public void setX_From(String x_From) {
        X_From = x_From;
    }

    public String getDate() {
        return Date;
    }

    public void setDate(String date) {
        Date = date;
    }

    public String getX_To() {
        return X_To;
    }

    public void setX_To(String x_To) {
        X_To = x_To;
    }

    public String getMessage_ID() {
        return Message_ID;
    }

    public void setMessage_ID(String message_ID) {
        Message_ID = message_ID;
    }

    public String getContent_Type() {
        return Content_Type;
    }

    public void setContent_Type(String content_Type) {
        Content_Type = content_Type;
    }

    public String getMime_Version() {
        return Mime_Version;
    }

    public void setMime_Version(String mime_Version) {
        Mime_Version = mime_Version;
    }
}
