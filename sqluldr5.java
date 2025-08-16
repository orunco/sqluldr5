/*
sqluldr2_bin    4s
normal          6s / 3.2GB  / text correct
cache_method    6s / 3.46GB / text correct
netty_bytebuf   5s / 1.6GB  / text correct
data_copy_ori   5s / 3.9GB  /
date_copy_mem   4s / 2.4GB  /

/jdk-17.0.2/bin/javac -classpath ./ojdbc8-12.2.0.1.jar:./netty-all-4.1.63.Final.jar:./ sqluldr5.java
/jdk-17.0.2/bin/java -classpath ./ojdbc8-12.2.0.1.jar:./netty-all-4.1.63.Final.jar:./ sqluldr5 oratest/oratest jdbc:oracle:thin:@127.0.0.1:1521:orcl "select * from t1"
 */

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.io.*;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.Charset;
import java.sql.*;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;

public class sqluldr5 {

    private static String sessionID = new SimpleDateFormat("MMdd_HHmmss_SSS").format(System.currentTimeMillis());
    private static String WORK_DIR = System.getProperty("user.dir") + "/data/" + sessionID + "/";
    private static BufferedWriter logWriter = null;
    private static int MAX_ONE_FILE = 1024 * 1024 * 1024;
    private static Integer batchInOneFile = null; //每1GB记录一个新文件，值太小日志会爆
    private static int batchFetchSize = 2 * 1000; //每X记录写一次文件
    private static int progressReport = 10 * 10000; //每X万汇报一次进度
    private static int bytebufSize = 1000 * batchFetchSize;
    private static SimpleDateFormat logFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    private static DecimalFormat FORMAT_DECIMAL_ONE = new DecimalFormat("0.0");

    private static Connection conn = null;
    private static Statement stmt = null;
    private static ResultSet rs = null;
    private static DBCharacterEncoding encoding = DBCharacterEncoding.UNKNOW;
    private static boolean hasValue;
    private static boolean[] isStringMethodCache = null;
    private static StringBuilder colBuilder = null;
    private static long rowCount = 0;
    private static long position = 0;
    private static long totalPosition = 0;

    private static RandomAccessFile localAccessFile = null;
    private static FileChannel localFileChannel = null;
    private static ByteBuf buffer = Unpooled.buffer(bytebufSize);
    private static Long fileID = 0L;
    private static String localFileName = "";

    public static void main(String[] args) throws Exception {

        if (System.getProperty("java.version").indexOf("17") != 0) {
            log("jdk must 17.x");
            return;
        }

        assert batchInOneFile >= batchFetchSize;
        assert batchFetchSize >= 2 * 1000;
        assert progressReport >= 10 * 1000;

        if (args.length != 3) {
            System.out.println("Help: sqluldr5 user/password url query ");
            System.out.println("Help: url like jdbc:oracle:thin:@127.0.0.1:1521:orcl ");
            System.out.println("Error:         args.length != 3");
            return;
        }

        String[] userPassArr = args[0].split("/");
        if (userPassArr.length != 2) {
            System.out.println("Help: sqluldr5 user/password url query ");
            System.out.println("Help: url like jdbc:oracle:thin:@127.0.0.1:1521:orcl ");
            System.out.println("Error:         user/password length != 2");
            return;
        }

        String user = userPassArr[0];
        String password = userPassArr[1];
        String url = args[1];
        String query = args[2];
        int fromPos = query.indexOf("from");
        String tableName = query.substring(fromPos + "from".length()).trim().split("\s+")[0];

        log("url=" + url);
        log("tableName=" + tableName);


        try {
            long beginTime = System.currentTimeMillis();

            Class.forName("oracle.jdbc.driver.OracleDriver");
            conn = DriverManager.getConnection(url, user, password);
            if (conn.getMetaData().getDriverMajorVersion() < 12) {
                System.out.println("Error: ojdbc must 12.x");
                return;
            }
            conn.setAutoCommit(false);
            conn.prepareStatement("ALTER SESSION SET DB_FILE_MULTIBLOCK_READ_COUNT=512");
            conn.prepareStatement("ALTER SESSION SET HASH_AREA_SIZE=" + 512 * 1048576);
            conn.prepareStatement("ALTER SESSION SET SORT_AREA_SIZE=" + 512 * 1048576);
            conn.prepareStatement("ALTER SESSION SET SORT_AREA_RETAINED_SIZE=" + 512 * 1048576);
            conn.prepareStatement("ALTER SESSION SET \"_sort_multiblock_read_count\"=128");
            conn.prepareStatement("ALTER SESSION SET \"_serial_direct_read\"=TRUE");

            stmt = conn.createStatement();
            stmt.setFetchSize(batchFetchSize);

            getDBCharacterEncoding();

            rs = stmt.executeQuery(query);
            try {
                new File(WORK_DIR).mkdirs();
            } catch (Exception ignored) {
            }

            logWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(
                    WORK_DIR + tableName + ".log", true)));
            log("query=" + query + " ok.");
            log("WORK_DIR=" + WORK_DIR);

            do {
                hasValue = rs.next();

                //只要有数据
                if (hasValue) {

                    if (isStringMethodCache == null) { //必然会有一条记录
                        isStringMethodCache = getMethodType(rs);
                        colBuilder = getColBuilder(rs);
                    }

                    if (batchInOneFile == null) {
                        adjustBatchInOneFile(rs);
                        log("batchInOneFile≈" + getReadableNumberIn10000(batchInOneFile)
                                + "(adjust) batchWriteSize=" + batchFetchSize
                                + " progressReport=" + getReadableNumberIn10000(progressReport));
                    }

                    //生成新文件的条件：源端有数据 且 批量写文件新的开始
                    if (hasValue && rowCount % batchInOneFile == 0) {
                        //new file , sum old size
                        totalPosition = totalPosition + position;

                        if (localFileChannel != null) localFileChannel.close();
                        if (localAccessFile != null) localAccessFile.close();

                        fileID = rowCount / batchInOneFile;
                        localFileName = WORK_DIR + tableName + "_" + fileID + ".txt";
                        localAccessFile = new RandomAccessFile(new File(localFileName), "rw");
                        localFileChannel = localAccessFile.getChannel();
                        position = 0;
                    }

                    rowCount++;

                    //转换一行(not contain line end) cec测试一下
                    transformRecord(rs, isStringMethodCache, buffer);

                    //行结尾加\n的条件：not命中batchInOneFile时
                    if (rowCount % batchInOneFile != 0) {
                        buffer.writeByte(PG_ROW_DELIMITER);
                    }
                }

                if (rowCount == 0 && hasValue == false) {
                    log("Table is empty.");
                    break;
                }

                //批量刷新的条件：到达了批量插入点 或者 文件批量插入点 或者 源端没有数据了，依次增强
                if (rowCount % batchFetchSize == 0 ||
                        rowCount % batchInOneFile == 0 ||
                        hasValue == false) {

                    if (rowCount % progressReport == 0) {
                        long curCost = (System.currentTimeMillis() - beginTime) / 1000;
                        if (curCost == 0) {
                            curCost = 1;
                        }

                        printProgress(getReadableNumberIn10000(rowCount) + " row, " + curCost + " sec");
                    }

                    position = executeBatchWriteToFile(localFileChannel, buffer, position);
                    buffer.clear(); // 调用clear之后，writerindex和readerinde全部复位为0。它不会清除缓冲区内容（例如，用填充0），而只是清除两个指针。
                }

                //提交文件
                if (hasValue == false || rowCount % batchInOneFile == 0) {
                    log("Generate " + localFileName + " ok.");
                }

            } while (hasValue);

            generateTargetPGCopySQL(tableName, colBuilder, fileID);

            long curCost = (System.currentTimeMillis() - beginTime) / 1000;
            if (curCost == 0) {
                curCost = 1;
            }
            totalPosition = totalPosition + position;

            log(rowCount + " ≈ " + getReadableNumberIn10000(rowCount) + " row, "
                    + " ≈ " + (totalPosition / 1024 / 1024) + " MBytes , "
                    + curCost + " sec, speed is "
                    + (totalPosition / 1024 / 1024 / curCost) + " MB/s");

        } catch (Exception e) {
            log(e.toString());
            e.printStackTrace();
        } finally {
            if (rs != null) rs.close();
            if (stmt != null) stmt.close();
            if (conn != null) conn.close();
            if (localFileChannel != null) localFileChannel.close();
            if (localAccessFile != null) localAccessFile.close();
            if (logWriter != null) logWriter.close();
            if (buffer != null) buffer.release();
        }
    }

    private static StringBuilder getColBuilder(ResultSet rs) throws Exception {
        StringBuilder colBuilder = new StringBuilder();
        for (int i = 1; i < rs.getMetaData().getColumnCount() + 1; i++) {
            String target_col = rs.getMetaData().getColumnLabel(i).toUpperCase();
            switch (rs.getMetaData().getColumnType(i)) {
                case Types.VARCHAR:
                case Types.LONGVARCHAR:
                case Types.NVARCHAR:
                case Types.LONGNVARCHAR:
                    colBuilder.append("\t").append(target_col + " text ").append(",\n");
                    break;
                case Types.REAL:
                    colBuilder.append("\t").append(target_col).append(" decimal ").append(",\n");
                    break;
                case Types.CHAR:
                case Types.NCHAR:
                    colBuilder.append("\t").append(target_col).append(" varchar(4000) ").append(",\n");
                    break;
                case Types.BIT:
                case Types.TINYINT:
                case Types.SMALLINT:
                case Types.INTEGER:
                case Types.BIGINT:
                case Types.FLOAT:
                case Types.DOUBLE:
                case Types.NUMERIC:
                case Types.DECIMAL:
                    colBuilder.append("\t").append(target_col).append(" decimal,\n");
                    break;
                case Types.DATE:
                case Types.TIMESTAMP:
                case Types.TIME:
                    colBuilder.append("\t").append(target_col).append(" timestamp without time zone ").append(",\n");
                    break;
                default:
                    // BINARY, VARBINARY, LONGVARBINARY, DISTINCT,
                    // ARRAY, STRUCT, REF, JAVA_OBJECT.
                    throw new Exception("not support type: " + rs.getMetaData().getColumnType(i) + " columnName : " + rs.getMetaData().getColumnName(i));
            }
        }
        colBuilder.deleteCharAt(colBuilder.length() - 2); // ,\n

        return colBuilder;
    }

    private static void getDBCharacterEncoding() throws Exception {
        ResultSet rs = stmt.executeQuery("select value from NLS_DATABASE_PARAMETERS where parameter='NLS_CHARACTERSET'");
        rs.next();
        try {
            encoding = Enum.valueOf(DBCharacterEncoding.class, rs.getString(1));
        } catch (Exception ex) {
        }
        if (encoding == DBCharacterEncoding.UNKNOW) {
            throw new Exception("无法获取数据库编码格式.");
        }
    }

    private static void generateTargetPGCopySQL(String tableName,
                                                StringBuilder colBuilder,
                                                long fileID) throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("drop table if exists ").append(tableName).append(";\n");
        sb.append("create table ").append(tableName).append("\n");
        sb.append("(\n");
        sb.append(colBuilder);
        sb.append(");").append("\n");

        for (long i = 0; i <= fileID; i++) {
            //copy a_symbol from '/var/lib/pgsql/data/0616_085532_466/a_symbol_0.txt' (format 'text',DELIMITER E'\t', NULL E'\\N',ENCODING 'utf8', HEADER 'false');
            sb.append("copy ").append(tableName).append(" from '").append(WORK_DIR);
            sb.append(tableName).append("_").append(i).append(".txt' ");
            sb.append("(format 'text',DELIMITER E'\\t', NULL E'\\\\N',ENCODING 'utf8', HEADER 'false');\n");
        }

        BufferedWriter targetMeta = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(
                WORK_DIR + tableName + ".sql", true)));
        targetMeta.write(sb.toString());
        targetMeta.close();
    }

    private static void adjustBatchInOneFile(ResultSet rs) throws Exception {
        int firstLineLen = 1;

        for (int i = 1; i < rs.getMetaData().getColumnCount() + 1; i++) {
            if (rs.getObject(i) == null) {
                firstLineLen += 2;
            } else {
                firstLineLen += rs.getString(i).getBytes().length;
            }
            firstLineLen += 1;
        }

        int estimateByMethodLen = 1;
        ResultSetMetaData rsMetaData = rs.getMetaData();
        for (int i = 1; i < rsMetaData.getColumnCount() + 1; i++) {
            switch (rsMetaData.getColumnType(i)) {
                case Types.BIT:
                case Types.BOOLEAN:
                    estimateByMethodLen += 1;
                    break;
                case Types.INTEGER:
                case Types.TINYINT:
                case Types.SMALLINT:
                case Types.BIGINT:
                case Types.REAL:
                case Types.FLOAT:
                    estimateByMethodLen += 4;
                    break;
                case Types.DOUBLE:
                case Types.NUMERIC:
                case Types.DECIMAL:
                    estimateByMethodLen += 8;
                    break;
                case Types.DATE:
                case Types.TIME:
                case Types.TIMESTAMP:
                    estimateByMethodLen += 4;
                    break;
                case Types.BINARY:
                case Types.VARCHAR:
                case Types.CHAR:
                case Types.LONGVARCHAR:
                case Types.NVARCHAR:
                case Types.NCHAR:
                case Types.LONGNVARCHAR:
                    estimateByMethodLen += 16; //maybe
                    break;
                default:
                    // BINARY, VARBINARY, LONGVARBINARY, DISTINCT,
                    // ARRAY, STRUCT, REF, JAVA_OBJECT.
                    throw new Exception("not support type: " + rsMetaData.getColumnType(i) + " columnName : " + rsMetaData.getColumnName(i));
            }
            estimateByMethodLen += 1;
        }

        batchInOneFile = MAX_ONE_FILE / ((firstLineLen + estimateByMethodLen) / 2);
    }

    private static boolean[] getMethodType(ResultSet rs) throws Exception {
        boolean[] result = new boolean[rs.getMetaData().getColumnCount()]; //错位一下
        ResultSetMetaData rsMetaData = rs.getMetaData();
        for (int i = 1; i < rsMetaData.getColumnCount() + 1; i++) {
            switch (rsMetaData.getColumnType(i)) {
                case Types.BIT:
                case Types.BOOLEAN:
                case Types.INTEGER:
                case Types.TINYINT:
                case Types.SMALLINT:
                case Types.BIGINT:
                case Types.REAL:
                case Types.FLOAT:
                case Types.DOUBLE:
                case Types.NUMERIC:
                case Types.DECIMAL:
                case Types.DATE:
                case Types.TIME:
                case Types.TIMESTAMP:
                case Types.BINARY:
                    result[i - 1] = false;
                    break;
                case Types.VARCHAR:
                case Types.CHAR:
                case Types.LONGVARCHAR:
                case Types.NVARCHAR:
                case Types.NCHAR:
                case Types.LONGNVARCHAR:
                    result[i - 1] = true;
                    break;
                default:
                    // BINARY, VARBINARY, LONGVARBINARY, DISTINCT,
                    // ARRAY, STRUCT, REF, JAVA_OBJECT.
                    throw new Exception("not support type: " + rsMetaData.getColumnType(i) + " columnName : " + rsMetaData.getColumnName(i));
            }
        }

        return result;
    }

    //关键函数，高度优化，减少判断，一次遍历
    static char PG_SLASH = '\\';
    static char PG_NULL_N = 'N';
    static char PG_FIELD_DELIMITER = '\t';
    static char PG_ROW_DELIMITER = '\n';
    static byte[] PG_SLASH_5C = new byte[]{'\\', 'x', '5', 'C'}; // \\
    static byte[] PG_SLASH_4E = new byte[]{'\\', 'x', '4', 'E'}; // N
    static byte[] PG_SLASH_09 = new byte[]{'\\', 'x', '0', '9'}; // \t
    static byte[] PG_SLASH_0A = new byte[]{'\\', 'x', '0', 'A'}; // \n
    static byte[] PG_SLASH_0D = new byte[]{'\\', 'x', '0', 'D'}; // \r

    public static void transformRecord(ResultSet sourceRS,
                                       boolean[] stringMethods,
                                       ByteBuf buffer) throws Exception {
        for (int i = 1; i <= stringMethods.length; i++) {
            if (sourceRS.getObject(i) == null) {
                buffer.writeByte(PG_SLASH);
                buffer.writeByte(PG_NULL_N);
            } else {
                if (stringMethods[i - 1]) {
                    if (encoding == DBCharacterEncoding.US7ASCII) {
                        try {
                            escapeString((new String(rs.getString(i).getBytes(Charset.forName("ISO-8859-1")),
                                    Charset.forName("GBK"))).getBytes(), buffer);
                        } catch (Exception ignore) {
                        }
                    } else {
                        escapeString(rs.getBytes(i), buffer);
                    }
                } else {
                    buffer.writeBytes(sourceRS.getObject(i).toString().getBytes());
                }
            }

            if (i != stringMethods.length) {
                buffer.writeByte(PG_FIELD_DELIMITER);
            }
        }
    }

    final public static void escapeString(byte[] field, ByteBuf buffer) {
        for (int i = 0; i < field.length; i++) {
            if (field[i] == 0x5C) { //aString.replace("\\N", "\\x5C\\4E"); 这里是2个字符 出现了第一个\
                if (i < field.length - 1 && field[i + 1] == 0x4E) { // 上面这个\至少是倒数第二 且 下一个字符是N
                    buffer.writeBytes(PG_SLASH_5C);
                    buffer.writeBytes(PG_SLASH_4E);
                    i++; //往前一步 因为一次性处理了2个字符(原来的下标)
                } else {
                    buffer.writeBytes(PG_SLASH_5C);     //aString.replace("\\", "\\x5C")
                }
            } else if (field[i] == 0x09) {              //aString.replace("\t", "\\x09");
                buffer.writeBytes(PG_SLASH_09);
            } else if (field[i] == 0x0A) {              //aString.replace("\n", "\\x0A");
                buffer.writeBytes(PG_SLASH_0A);
            } else if (field[i] == 0x0D) {              //aString.replace("\r", "\\x0D");
                buffer.writeBytes(PG_SLASH_0D);
            } else if (field[i] == 0x0) {               //aString.replace("\u0000", " ");
                buffer.writeByte(' ');
            } else {
                buffer.writeByte(field[i]);
            }
        }
    }

    public static long executeBatchWriteToFile(FileChannel fileChannel, ByteBuf buffer,
                                               long position) throws Exception {
        ByteArrayInputStream targetByteArrayInputStream = new ByteArrayInputStream(buffer.array());
        ReadableByteChannel targetReadableByteChannel = Channels.newChannel(targetByteArrayInputStream);
        fileChannel.transferFrom(targetReadableByteChannel, position, buffer.writerIndex());
        position += buffer.writerIndex();
        return position;
    }

    public static void log(String info) throws IOException {
        String tmp = logFormatter.format(System.currentTimeMillis()) + " " + info + " \n";
        System.out.print(tmp);
        if (logWriter != null) {
            logWriter.write(tmp);
        }
    }

    public static void printProgress(String info) {
        //2017-03-10 13:24:11.372 [pool-2-thread-11] XXX
        System.out.print('\r'); //回退
        System.out.print(logFormatter.format(System.currentTimeMillis()) + " " + info + " ");
    }

    public static String getReadableNumberIn10000(double x) {
        double res = x / 10000d;
        if (res < 1) {
            return new DecimalFormat("0").format(x);
        }

        String tmp = FORMAT_DECIMAL_ONE.format(res);
        if (tmp.substring(tmp.length() - 1).equalsIgnoreCase("0")) {
            return tmp.substring(0, tmp.length() - 2) + "W";
        } else {
            return tmp + "W";
        }
    }

    private enum DBCharacterEncoding {
        UNKNOW,
        AL32UTF8,
        ZHS16GBK,
        US7ASCII
    }
}