package com.anur.core.log.common;

import java.io.File;
import java.text.NumberFormat;
/**
 * Created by Anur IjuoKaruKas on 2019/3/4
 */
public class LogCommon {

    /** a log file */
    public static final String LogFileSuffix = ".log";

    /** an index file */
    public static final String IndexFileSuffix = ".index";

    /** A temporary file used when swapping files into the log */
    public static final String SwapFileSuffix = ".swap";

    /**
     * Construct a log file name in the given dir with the given base offset
     *
     * @param dir The directory in which the log will reside
     * @param offset The base offset of the log file
     */
    public static File logFilename(File dir, long offset) {
        return new File(dir, filenamePrefixFromOffset(offset) + LogFileSuffix);
    }

    /**
     * Construct an index file name in the given dir using the given base offset
     *
     * @param dir The directory in which the log will reside
     * @param offset The base offset of the log file
     */
    public static File indexFilename(File dir, long offset) {
        return new File(dir, filenamePrefixFromOffset(offset) + IndexFileSuffix);
    }

    public static File dirName(File baseDir, long generation) {
        return new File(baseDir + "\\" + filenamePrefixFromOffset(generation));
    }

    /**
     * Make log segment file name from offset bytes. All this does is pad out the offset number with zeros
     * so that ls sorts the files numerically.
     *
     * @param offset The offset to use in the file name
     *
     * @return The filename
     */
    public static String filenamePrefixFromOffset(long offset) {
        NumberFormat nf = NumberFormat.getInstance();
        nf.setMinimumIntegerDigits(20);
        nf.setMaximumFractionDigits(0);
        nf.setGroupingUsed(false);
        return nf.format(offset);
    }
}
