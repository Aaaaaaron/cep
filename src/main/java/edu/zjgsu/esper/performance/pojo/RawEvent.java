package edu.zjgsu.esper.performance.pojo;

/**
 * Created by AH on 2017/1/4.
 */
public class RawEvent {
    String catBehavior;
    String catOutcome;
    String srcAddress;
    String deviceCat;
    String srcUsername;
    String catObject;
    String destAddress;
    String appProtocol;

    public RawEvent ( String catBehavior, String catOutcome, String srcAddress, String deviceCat, String srcUsername, String catObject, String destAddress, String appProtocol ) {
        this.catBehavior = catBehavior;
        this.catOutcome = catOutcome;
        this.srcAddress = srcAddress;
        this.deviceCat = deviceCat;
        this.srcUsername = srcUsername;
        this.catObject = catObject;
        this.destAddress = destAddress;
        this.appProtocol = appProtocol;
    }

    public String getCatBehavior () {
        return catBehavior;
    }

    public void setCatBehavior ( String catBehavior ) {
        this.catBehavior = catBehavior;
    }

    public String getCatOutcome () {
        return catOutcome;
    }

    public void setCatOutcome ( String catOutcome ) {
        this.catOutcome = catOutcome;
    }

    public String getSrcAddress () {
        return srcAddress;
    }

    public void setSrcAddress ( String srcAddress ) {
        this.srcAddress = srcAddress;
    }

    public String getDeviceCat () {
        return deviceCat;
    }

    public void setDeviceCat ( String deviceCat ) {
        this.deviceCat = deviceCat;
    }

    public String getSrcUsername () {
        return srcUsername;
    }

    public void setSrcUsername ( String srcUsername ) {
        this.srcUsername = srcUsername;
    }

    public String getCatObject () {
        return catObject;
    }

    public void setCatObject ( String catObject ) {
        this.catObject = catObject;
    }

    public String getDestAddress () {
        return destAddress;
    }

    public void setDestAddress ( String destAddress ) {
        this.destAddress = destAddress;
    }

    public String getAppProtocol () {
        return appProtocol;
    }

    public void setAppProtocol ( String appProtocol ) {
        this.appProtocol = appProtocol;
    }
}
