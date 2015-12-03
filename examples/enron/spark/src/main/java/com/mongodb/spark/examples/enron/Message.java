package com.mongodb.spark.examples.enron;

/**
 * Created by bryan on 12/3/15.
 */

public class Message {

    private String id;
    private String body;
    private String mailbox;
    private String filename;
    private String xCC;
    private String subject;
    private String xFolder;
    private String contentTransferEncoding;
    private String xBcc;
    private String to;
    private String xOrigin;
    private String xFilename;
    private String xFrom;
    private String date;
    private String xTo;
    private String messageID;
    private String contentType;
    private String mimeVersion;

    public String getId() {
        return id;
    }

    public void setId(final String id) {
        this.id = id;
    }

    public String getBody() {
        return body;
    }

    public void setBody(final String body) {
        this.body = body;
    }

    public String getMailbox() {
        return mailbox;
    }

    public void setMailboxfinal(final String mailbox) {
        this.mailbox = mailbox;
    }

    public String getFilename() {
        return filename;
    }

    public void setFilename(final String filename) {
        this.filename = filename;
    }

    public String getxCC() {
        return xCC;
    }

    public void setxCC(final String xCC) {
        this.xCC = xCC;
    }

    public String getSubject() {
        return subject;
    }

    public void setSubject(final String subject) {
        this.subject = subject;
    }

    public String getxFolder() {
        return xFolder;
    }

    public void setxFolder(final String xFolder) {
        this.xFolder = xFolder;
    }

    public String getContentTransferEncoding() {
        return contentTransferEncoding;
    }

    public void setContentTransferEncoding(final String contentTransferEncoding) {
        this.contentTransferEncoding = contentTransferEncoding;
    }

    public String getxBcc() {
        return xBcc;
    }

    public void setxBcc(final String xBcc) {
        this.xBcc = xBcc;
    }

    public String getTo() {
        return to;
    }

    public void setTo(final String to) {
        this.to = to;
    }

    public String getxOrigin() {
        return xOrigin;
    }

    public void setxOrigin(final String xOrigin) {
        this.xOrigin = xOrigin;
    }

    public String getxFilename() {
        return xFilename;
    }

    public void setxFilename(final String xFilename) {
        this.xFilename = xFilename;
    }

    public String getxFrom() {
        return xFrom;
    }

    public void setxFrom(final String xFrom) {
        this.xFrom = xFrom;
    }

    public String getDate() {
        return date;
    }

    public void setDate(final String date) {
        this.date = date;
    }

    public String getxTo() {
        return xTo;
    }

    public void setxTo(final String xTo) {
        this.xTo = xTo;
    }

    public String getMessageID() {
        return messageID;
    }

    public void setMessageID(final String messageID) {
        this.messageID = messageID;
    }

    public String getContentType() {
        return contentType;
    }

    public void setContentType(final String contentType) {
        this.contentType = contentType;
    }

    public String getMimeVersion() {
        return mimeVersion;
    }

    public void setMimeVersion(final String mimeVersion) {
        this.mimeVersion = mimeVersion;
    }
}