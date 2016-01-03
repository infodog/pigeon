package net.xinshi.pigeon.dumpload.loaddata;

/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-5-11
 * Time: 下午4:01
 * To change this template use File | Settings | File Templates.
 */

public class HexRecord {
    String name;
    String value;
    byte[] bytes;
    String isCompressed = null;
    String isString = null;

    String fileName;
    long curLine;

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public long getCurLine() {
        return curLine;
    }

    public void setCurLine(long curLine) {
        this.curLine = curLine;
    }

    public HexRecord(String name, String value) {
        this.name = name;
        this.value = value;
    }

    public HexRecord(String name, String value, byte[] bytes, String compressed, String string) {
        this.name = name;
        this.value = value;
        this.bytes = bytes;
        isCompressed = compressed;
        isString = string;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public byte[] getBytes() {
        return bytes;
    }

    public String getCompressed() {
        return isCompressed;
    }

    public String getString() {
        return isString;
    }

}
