package com.example.demo.client;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.example.demo.entity.Sku;
import com.example.demo.entity.Table;
import com.example.demo.util.KafkaUtils;
import com.example.demo.util.ReflectionUtil;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.commons.lang.SystemUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;

import java.lang.reflect.InvocationTargetException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by liuxing on 2017/11/22.
 */
public class AbstractCanalClient {
    protected final static Logger logger = LoggerFactory.getLogger(AbstractCanalClient.class);
    protected static final String SEP = SystemUtils.LINE_SEPARATOR;
    protected static final String DATE_FORMAT  = "yyyy-MM-dd HH:mm:ss";
    protected volatile boolean running = false;
    protected Thread.UncaughtExceptionHandler handler = new Thread.UncaughtExceptionHandler() {
        public void uncaughtException(Thread t, Throwable e) {
            logger.error("parse events has an error", e);
        }
    };
    protected Thread thread = null;
    protected CanalConnector connector;
    protected static String context_format = null;
    protected static String row_format = null;
    protected static String transaction_format = null;
    protected String destination;

    static {
        context_format = SEP + "****************************************************" + SEP;
        context_format += "* Batch Id: [{}] ,count : [{}] , memsize : [{}] , Time : {}" + SEP;
        context_format += "* Start : [{}] " + SEP;
        context_format += "* End : [{}] " + SEP;
        context_format += "****************************************************" + SEP;
        row_format = SEP
                + "----------------> binlog[{}:{}] , name[{},{}] , eventType : {} , executeTime : {} , delay : {}ms"
                + SEP;
        transaction_format = SEP + "================> binlog[{}:{}] , executeTime : {} , delay : {}ms" + SEP;

    }

    public AbstractCanalClient(String destination){
        this(destination, null);
    }

    public AbstractCanalClient(String destination, CanalConnector connector){
        this.destination = destination;
        this.connector = connector;
    }

    protected void start() {
        Assert.notNull(connector, "connector is null");
        thread = new Thread(new Runnable() {
            public void run() {
                process();
            }
        });
        thread.setUncaughtExceptionHandler(handler);
        thread.start();
        running = true;
    }

    protected void stop() {
        if (!running) {
            return;
        }
        running = false;
        if (thread != null) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                // ignore
            }
        }
        MDC.remove("destination");
    }

    protected void process() {
        int batchSize = 5 * 1024;
        while (running) {
            try {
                MDC.put("destination", destination);
                connector.connect();
                connector.subscribe();
                while (running) {
                    Message message = connector.getWithoutAck(batchSize); // 获取指定数量的数据
                    long batchId = message.getId();
                    int size = message.getEntries().size();
                    if (batchId == -1 || size == 0) {
                        // try {
                        // Thread.sleep(1000);
                        // } catch (InterruptedException e) {
                        // }
                    } else {
                        printSummary(message, batchId, size);
                        printEntry(message.getEntries());
                    }
                    connector.ack(batchId); // 提交确认
                    // connector.rollback(batchId); // 处理失败, 回滚数据
                }
            } catch (Exception e) {
                logger.error("process error!", e);
            } finally {
                connector.disconnect();
                MDC.remove("destination");
            }
        }
    }

    private void printSummary(Message message, long batchId, int size) {
        long memsize = 0;
        for (CanalEntry.Entry entry : message.getEntries()) {
            memsize += entry.getHeader().getEventLength();
        }

        String startPosition = null;
        String endPosition = null;
        if (!CollectionUtils.isEmpty(message.getEntries())) {
            startPosition = buildPositionForDump(message.getEntries().get(0));
            endPosition = buildPositionForDump(message.getEntries().get(message.getEntries().size() - 1));
        }

        SimpleDateFormat format = new SimpleDateFormat(DATE_FORMAT);
        logger.info(context_format, new Object[] { batchId, size, memsize, format.format(new Date()), startPosition,
                endPosition });
    }

    protected String buildPositionForDump(CanalEntry.Entry entry) {
        long time = entry.getHeader().getExecuteTime();
        Date date = new Date(time);
        SimpleDateFormat format = new SimpleDateFormat(DATE_FORMAT);
        return entry.getHeader().getLogfileName() + ":" + entry.getHeader().getLogfileOffset() + ":"
                + entry.getHeader().getExecuteTime() + "(" + format.format(date) + ")";
    }

    protected void printEntry(List<CanalEntry.Entry> entrys)throws Exception {
        for (CanalEntry.Entry entry : entrys) {
            long executeTime = entry.getHeader().getExecuteTime();
            long delayTime = new Date().getTime() - executeTime;

            if (entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONBEGIN || entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONEND) {
                if (entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONBEGIN) {
                    CanalEntry.TransactionBegin begin = null;
                    try {
                        begin = CanalEntry.TransactionBegin.parseFrom(entry.getStoreValue());
                    } catch (InvalidProtocolBufferException e) {
                        throw new RuntimeException("parse event has an error , data:" + entry.toString(), e);
                    }
                    // 打印事务头信息，执行的线程id，事务耗时
                    logger.info(transaction_format,
                            new Object[] { entry.getHeader().getLogfileName(),
                                    String.valueOf(entry.getHeader().getLogfileOffset()),
                                    String.valueOf(entry.getHeader().getExecuteTime()), String.valueOf(delayTime) });
                    logger.info(" BEGIN ----> Thread id: {}", begin.getThreadId());
                } else if (entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONEND) {
                    CanalEntry.TransactionEnd end = null;
                    try {
                        end = CanalEntry.TransactionEnd.parseFrom(entry.getStoreValue());
                    } catch (InvalidProtocolBufferException e) {
                        throw new RuntimeException("parse event has an error , data:" + entry.toString(), e);
                    }
                    // 打印事务提交信息，事务id
                    logger.info("----------------\n");
                    logger.info(" END ----> transaction id: {}", end.getTransactionId());
                    logger.info(transaction_format,
                            new Object[] { entry.getHeader().getLogfileName(),
                                    String.valueOf(entry.getHeader().getLogfileOffset()),
                                    String.valueOf(entry.getHeader().getExecuteTime()), String.valueOf(delayTime) });
                }
                continue;
            }

            if (entry.getEntryType() == CanalEntry.EntryType.ROWDATA) {
                CanalEntry.RowChange rowChage = null;
                try {
                    rowChage = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
                } catch (Exception e) {
                    throw new RuntimeException("parse event has an error , data:" + entry.toString(), e);
                }
                CanalEntry.EventType eventType = rowChage.getEventType();
                logger.info(row_format,
                        new Object[] { entry.getHeader().getLogfileName(),
                                String.valueOf(entry.getHeader().getLogfileOffset()), entry.getHeader().getSchemaName(),
                                entry.getHeader().getTableName(), eventType,
                                String.valueOf(entry.getHeader().getExecuteTime()), String.valueOf(delayTime) });

                if (eventType == CanalEntry.EventType.QUERY || rowChage.getIsDdl()) {
                    logger.info(" sql ----> " + rowChage.getSql() + SEP);
                    continue;
                }
                String tableName = entry.getHeader().getTableName();//获取表名
                Class cls = Table.valueOf(tableName.toUpperCase()).getCls();//根据表名获取class
                List<Object> list = new ArrayList<>();
                for (CanalEntry.RowData rowData : rowChage.getRowDatasList()) {
                    Object o = ReflectionUtil.newInstance(cls);//实例化对象
                    if (eventType == CanalEntry.EventType.DELETE) {
                        printColumn(rowData.getBeforeColumnsList(), o);
                    } else if (eventType == CanalEntry.EventType.INSERT) {
                        printColumn(rowData.getAfterColumnsList(), o);
                    } else {//update
                        printColumn(rowData.getAfterColumnsList(), o);
                    }
                    list.add(o);
                    String string = (String) ReflectionUtil.getFileValue(o, "toString");
                    System.out.println(string);
                    KafkaUtils.writeToKafka(tableName, string);
                }
            }
        }
    }

    /**
     * 正则表达式获取mysql得类型
     * varchar(50) -》 varchar
     * @param mysqlType
     * @return
     */
    private String getMysqlType(String mysqlType){
        Pattern p = Pattern.compile("[a-z]+");
        Matcher matcher = p.matcher(mysqlType);
        while(matcher.find()){
            return matcher.group().toUpperCase();
        }
        return null;
    }

    /**
     * 根据mysql类型获取java类型
     * varchar -》 String.class
     * @param mysqlType
     * @return
     */
    private Class getJavaType(String mysqlType){
        if("CHAR".equals(mysqlType)){
            return Character.class;
        }else if("VARCHAR".equals(mysqlType)){
            return String.class;
        }else if("NUMERIC".equals(mysqlType)){
            return Integer.class;
        }else if("DECIMAL".equals(mysqlType)){
            return Integer.class;
        }else if("BOOLEAN".equals(mysqlType)){
            return Boolean.class;
        }else if("TINYINT".equals(mysqlType)){
            return Byte.class;
        }else if("INTEGER".equals(mysqlType)){
            return Integer.class;
        }else if("INT".equals(mysqlType)){
            return Integer.class;
        }else if("BIGINT".equals(mysqlType)){
            return Long.class;
        }else if("REAL".equals(mysqlType)){
            return Float.class;
        }else if("FLOAT".equals(mysqlType)){
            return Float.class;
        }else if("DOUBLE".equals(mysqlType)){
            return Double.class;
        }else if("TIMESTAMP".equals(mysqlType)){
            return Date.class;
        }else if("DATETIME".equals(mysqlType)){
            return Date.class;
        }
        return String.class;
    }

    protected void printColumn(List<CanalEntry.Column> columns, Object o) {
        for (CanalEntry.Column column : columns) {
            StringBuilder builder = new StringBuilder();
            builder.append(column.getName() + " : " + column.getValue());
            builder.append("    type=" + column.getMysqlType());
            if (column.getUpdated()) {
                builder.append("    update=" + column.getUpdated());
            }
            builder.append(SEP);
            logger.info(builder.toString());

            String name = column.getName();
            name = "set" + name.substring(0,1).toUpperCase() + name.substring(1,name.length());
            Class javaType = getJavaType(getMysqlType(column.getMysqlType()));
            String value = column.getValue();
            Object o1 = ReflectionUtil.getParamValue(javaType, value);
            try {
                ReflectionUtil.setFileValue(o, name, o1);
            }  catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public void setConnector(CanalConnector connector) {
        this.connector = connector;
    }
}

