package com.jd.blockchain.kvdb.server;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

public class IOTest2 {

    private static final String HD = "/home/imuge/";
    private static final String SSD = "/";
    private static final String W1024S = "Jirm49TciVX4RbLoTToCVBa2xek31GsmFut23r2LQ6aTrubVqPN88DX4q9wt2JOSYFsG1k8tjpqSFnRHl33jeha6JMd5dRk2yMQ5wEeKmrwkLVkz2SgSJQtbHYbmGqW1913woS8k2JL4tAqIw6cigp59of3rQmtEos40fwRqBWgNELPkQ5k93L12C8bjHe2HYo0jxl0nXU5soBMYwEKAYw9NtfkrB7NVNRwItyXYq9y8lZPX4xWTjBPxxa9UHzkQIJuEvXyYw1Wrc4ZcC0GlqFnAoqMMYDKOMukLQBt86dsSeMcixBzmv1Fgtoix7woTlZBRCv80OeIMSUXYQ35peJFpuWNGbtiOxVJzZKXCM0RfbWuTpqdC0j3aKG9fpRXkbEB4cxidIjQqBKGrJuMGIjiwKcqusbTvh7PCrEcwxkjiwTBdEoYOWWtAaprW3w3si61PJRuOA944WnaHNT4GgTFS2Nktu9kEib0sWzQliGPNimse23ROl134bESDvyQ6ZF1eVT7oeVQ6Foc5zCJnpUl2e1bBTbYRFgEOB70M6bU2OFfYnA96oWTS30SvXjERpjPmOHvNeaZlFr0sI6cF1N10vXMFuKn1QGjGD8UjVkVnuTz7GLJlcOpRNugFnlnklK6mcFpocoh5u3eD2ta5SU9pPFaLdazWuDBb2y1ONx8A6VOKqEKdBorV8mN9Uk9tpLJXSrahReIexgPkOLRgbMbWC3dAKrDmCmL9vSwoJmb5H2PtlGURPhtiTazVPKEGH45omAA14SWMDe8uXjVCYJ1WXNv1ZLNfYhET5uoGaNob66rSC4k0RujujQbk8GaOGVFe6EV8sxI2f8Qc37GLY0XlUTxBuoGyxf3AIMSN29BzPdtaBNH0rbYUFbWyCYbuEnmNHpUcqhxcfErslgh6D7U74OxGvnoOpovPmHQ9Up78H5rhk6USQNAmOtpweh40QDRNFsYYKDzvTCHPaFc1vTpEsDSTUWwGuviviVbgkluLFmw4wR7NhqOYDNIcF6k9";
    private static final byte[] W1024BS = "Jirm49TciVX4RbLoTToCVBa2xek31GsmFut23r2LQ6aTrubVqPN88DX4q9wt2JOSYFsG1k8tjpqSFnRHl33jeha6JMd5dRk2yMQ5wEeKmrwkLVkz2SgSJQtbHYbmGqW1913woS8k2JL4tAqIw6cigp59of3rQmtEos40fwRqBWgNELPkQ5k93L12C8bjHe2HYo0jxl0nXU5soBMYwEKAYw9NtfkrB7NVNRwItyXYq9y8lZPX4xWTjBPxxa9UHzkQIJuEvXyYw1Wrc4ZcC0GlqFnAoqMMYDKOMukLQBt86dsSeMcixBzmv1Fgtoix7woTlZBRCv80OeIMSUXYQ35peJFpuWNGbtiOxVJzZKXCM0RfbWuTpqdC0j3aKG9fpRXkbEB4cxidIjQqBKGrJuMGIjiwKcqusbTvh7PCrEcwxkjiwTBdEoYOWWtAaprW3w3si61PJRuOA944WnaHNT4GgTFS2Nktu9kEib0sWzQliGPNimse23ROl134bESDvyQ6ZF1eVT7oeVQ6Foc5zCJnpUl2e1bBTbYRFgEOB70M6bU2OFfYnA96oWTS30SvXjERpjPmOHvNeaZlFr0sI6cF1N10vXMFuKn1QGjGD8UjVkVnuTz7GLJlcOpRNugFnlnklK6mcFpocoh5u3eD2ta5SU9pPFaLdazWuDBb2y1ONx8A6VOKqEKdBorV8mN9Uk9tpLJXSrahReIexgPkOLRgbMbWC3dAKrDmCmL9vSwoJmb5H2PtlGURPhtiTazVPKEGH45omAA14SWMDe8uXjVCYJ1WXNv1ZLNfYhET5uoGaNob66rSC4k0RujujQbk8GaOGVFe6EV8sxI2f8Qc37GLY0XlUTxBuoGyxf3AIMSN29BzPdtaBNH0rbYUFbWyCYbuEnmNHpUcqhxcfErslgh6D7U74OxGvnoOpovPmHQ9Up78H5rhk6USQNAmOtpweh40QDRNFsYYKDzvTCHPaFc1vTpEsDSTUWwGuviviVbgkluLFmw4wR7NhqOYDNIcF6k9".getBytes();

    public static void main(String[] args) throws IOException {
        String ct = args.length > 0 ? args[0] : "0";
        String c;
        if (ct.equals("0")) {
            c = HD;
        } else {
            c = SSD;
        }
        String cas = args.length > 1 ? args[1] : "4";
        long time = 0;
        switch (cas) {
            case "1":
                time = writeBuffer(new File(c + "1.t"), false, false);
                break;
            case "2":
                time = writeBuffer(new File(c + "1.t"), true, false);
                break;
            case "3":
                time = writeBuffer(new File(c + "1.t"), true, true);
                break;
            case "4":
                time = writeFOut(new File(c + "2.t"), false, false);
                break;
            case "5":
                time = writeFOut(new File(c + "2.t"), true, false);
                break;
            case "6":
                time = writeFOut(new File(c + "2.t"), true, true);
                break;
            case "7":
                time = writeByteBuffer(new File(c + "3.t"), false, false);
                break;
            case "8":
                time = writeByteBuffer(new File(c + "3.t"), true, false);
                break;
            case "9":
                time = writeByteBuffer(new File(c + "3.t"), true, true);
                break;
            case "10":
                time = writeFileChannel(new File(c + "4.t"), false, false);
                break;
            case "11":
                time = writeFileChannel(new File(c + "4.t"), true, false);
                break;
            case "12":
                time = writeFileChannel(new File(c + "4.t"), true, true);
                break;
            case "13":
                time = writeMMap(new File(c + "5.t"), false, false);
                break;
            case "14":
                time = writeMMap(new File(c + "5.t"), true, false);
                break;
            case "15":
                time = writeMMap(new File(c + "5.t"), true, true);
                break;
        }
        System.out.println(time/1000d + "s");
        System.out.println("MB/s: " + ((1024 * 1024 * 1024d / 1000) / time));
    }

    static long writeBuffer(File file, boolean flush, boolean mdata) throws IOException {
        FileOutputStream fos = new FileOutputStream(file);
        FileChannel fc = fos.getChannel();
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fos));
        long start = System.currentTimeMillis();
        int i = 1024;
        while (i > 0) {
            for (int j = 0; j < 1024; j++) {
                writer.write(W1024S);
            }
            if (flush) {
                if (mdata) {
                    fc.force(true);
                } else {
                    fc.force(false);
                }
            }
            i--;
        }
        long end = System.currentTimeMillis();
        writer.close();
        fos.close();

        return end - start;
    }

    static long writeFOut(File file, boolean flush, boolean mdata) throws IOException {
        FileOutputStream fos = new FileOutputStream(file);
        FileChannel fc = fos.getChannel();
        long start = System.currentTimeMillis();
        int i = 1024;
        while (i > 0) {
            for (int j = 0; j < 1024; j++) {
                fos.write(W1024BS);
            }
            if (flush) {
                if (mdata) {
                    fc.force(true);
                } else {
                    fc.force(false);
                }
            }
            i--;
        }
        long end = System.currentTimeMillis();
        fc.close();
        fos.close();
        return end - start;
    }

    static long writeByteBuffer(File file, boolean flush, boolean mdata) throws IOException {
        FileOutputStream fos = new FileOutputStream(file);
        FileChannel fc = fos.getChannel();
        int times = 1024;
        ByteBuffer bbuf = ByteBuffer.allocate(1024 * times);
        long start = System.currentTimeMillis();
        int i = 1024;
        while (i > 0) {
            for (int j = 0; j < times; j++) {
                bbuf.put(W1024BS);
            }
            bbuf.flip();
            fc.write(bbuf);
            bbuf.clear();
            if (flush) {
                if (mdata) {
                    fc.force(true);
                } else {
                    fc.force(false);
                }
            }
            i--;
        }
        long end = System.currentTimeMillis();
        fc.close();
        fos.close();

        return end - start;
    }

    static long writeFileChannel(File file, boolean flush, boolean mdata) throws IOException {
        FileOutputStream fos = new FileOutputStream(file);
        FileChannel fc = fos.getChannel();
        int times = 1024;
        ByteBuffer bbuf = ByteBuffer.allocateDirect(1024 * times);
        long start = System.currentTimeMillis();
        int i = 1024;
        while (i > 0) {
            for (int j = 0; j < times; j++) {
                bbuf.put(W1024BS);
            }
            bbuf.flip();
            fc.write(bbuf);
            bbuf.clear();
            if (flush) {
                if (mdata) {
                    fc.force(true);
                } else {
                    fc.force(false);
                }
            }
            i--;
        }
        long end = System.currentTimeMillis();
        fc.close();
        fos.close();

        return end - start;
    }

    static long writeMMap(File file, boolean flush, boolean mdata) throws IOException {
        RandomAccessFile acf = new RandomAccessFile(file, "rw");
        FileChannel fc = acf.getChannel();
        long start = System.currentTimeMillis();
        int len = W1024BS.length * 1024;
        long offset = 0;
        int i = 1024;
        while (i > 0) {
            MappedByteBuffer mbuf = fc.map(FileChannel.MapMode.READ_WRITE, offset, len);
            for (int j = 0; j < 1024; j++) {
                mbuf.put(W1024BS);
            }
            if (flush) {
                if (mdata) {
                    fc.force(true);
                } else {
                    fc.force(false);
                }
            }
            offset = offset + len;
            i--;
        }
        long end = System.currentTimeMillis();
        fc.close();
        acf.close();

        return end - start;
    }

}
