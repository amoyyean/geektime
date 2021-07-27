package com.example;

import com.google.common.io.Resources;
import jdk.jfr.events.FileReadEvent;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.util.*;

public class HbaseApiOperation {
    // 获取hbase connection
    private Connection conn ;
    // 获取hbase admin hbase 管理员
    private Admin admin ;
    // hbase table
    private Table table ;
    // 统一命名空间名称
    private final static String tableNameSpace = "wanghuan" ;
    // 统一表名字
    private  final static String tableName="wanghuan:student" ;

    @Before
    public void init() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        // 设置ZK Server IP
//        conf.set("hbase.zookeeper.quorum", "47.101.206.249,47.101.216.12,47.101.204.23");
        conf.set("hbase.zookeeper.quorum", "192.168.2.100,192.168.2.101,192.168.2.102");
        // // 设置ZK node port
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        this.conn = ConnectionFactory.createConnection(conf);
        this.admin = conn.getAdmin();
        // 获取数据表信息
        this.table = this.conn.getTable(TableName.valueOf("hbase:meta"));
    }
    /**
     * 创建hbase 命名空间
     * @param
     * @throws IOException
     */
    @Test
    public void createNameSpace() throws IOException {
        NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create("wanghuan").build();
        this.admin.createNamespace(namespaceDescriptor);
        System.out.println("create namespace success！");

    }

    /**
     * 查询指定命名空间下的表
     * @param
     * @throws IOException
     */
    @Test
    public void listTableByNameSpace() throws IOException {
        TableName[] tableNames = this.admin.listTableNamesByNamespace(HbaseApiOperation.tableNameSpace);
        for (TableName tableName: tableNames) {
            System.out.println("table = "+tableName);
        }
    }

    /**
     * 查询所有的表
     * @param
     * @throws IOException
     */
    @Test
    public void listAllTable() throws IOException {
        TableName[] tableNames = this.admin.listTableNames();
        for (TableName tableName: tableNames) {
            System.out.println("table = "+tableName);
        }
    }

    public boolean isExistsTable(String tableName) throws IOException {
        // 判断传入的表名是否为空
        if (tableName==null)
            return false ;
        return this.admin.tableExists(TableName.valueOf(tableName));
    }

    /**
     * 创建表 ， 指定多个列族
     * @throws IOException
     * @param table
     * @param columnFamilys
     * @param versions
     */
    @Test
    public void createTable(String table, String[] columnFamilys, int versions) throws IOException {
        // 指定表名   表名：$your_name:student
        TableName tableName = TableName.valueOf(table);

        if(!this.admin.tableExists(tableName)){
            System.out.println("table not exists");
            //表构建者
            TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tableName);
            ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder;
            //表描述器
            TableDescriptor tableDescriptor;
            // 列族描述器
            ColumnFamilyDescriptor columnFamilyDescriptor;

            for(String columnFamily : columnFamilys){
                columnFamilyDescriptorBuilder = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(columnFamily));
                // 设置列族中数据VERSIONS最大为5
                columnFamilyDescriptorBuilder.setMaxVersions(versions);
                columnFamilyDescriptor = columnFamilyDescriptorBuilder.build();
                tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptor);
            }

            tableDescriptor = tableDescriptorBuilder.build();
            admin.createTable(tableDescriptor);
            if(admin.tableExists(tableName)){
                System.out.println(" create table success !");
            }
        }else{
            System.out.println("table exists");
        }
    }

    @Test
    public void insertData(String tableName , ArrayList<Student> studentArrayList) throws IOException {
        List<Put> puts = new ArrayList<Put>();
        // 若表不存在，结束任务
        if (!isExistsTable(tableName))
            return ;
        // Tabel负责记录相关的操作如增删改查等
        this.table = this.conn.getTable(TableName.valueOf(tableName));
        ///增加多行记录
        if (studentArrayList != null && studentArrayList.size() > 0) {
            for (Student stuMap : studentArrayList) {
                // 若当条信息的rowkey为空 丢弃该条数据
                if (stuMap.getStudent_id()==null)
                    continue;
                // Student_id 作为rowkey
                Put put = new Put(Bytes.toBytes(stuMap.getStudent_id().toString()));
                // 添加info 列簇信息 , 判断当前列信息为空时不处理
                if (stuMap.getName()!=null){
                    put.addColumn(Bytes.toBytes("info".toString()),
                            Bytes.toBytes(("name").toString()),
                            Bytes.toBytes(stuMap.getName().toString()));
                }
                put.addColumn(Bytes.toBytes("info".toString()),
                        Bytes.toBytes(("student_id").toString()),
                        Bytes.toBytes(stuMap.getStudent_id().toString()));
                if (stuMap.getClassInfo()!=null){
                    put.addColumn(Bytes.toBytes("info".toString()),
                            Bytes.toBytes(("class").toString()),
                            Bytes.toBytes(stuMap.getClassInfo().toString()));
                }

                // 添加score列簇信息
                if (stuMap.getUnderstanding()!=null){
                    put.addColumn(Bytes.toBytes("score".toString()),
                            Bytes.toBytes(("understanding").toString()),
                            Bytes.toBytes(stuMap.getUnderstanding().toString()));
                }
                if (stuMap.getUnderstanding()!=null){
                    put.addColumn(Bytes.toBytes("score".toString()),
                            Bytes.toBytes(("programming").toString()),
                            Bytes.toBytes(stuMap.getUnderstanding().toString()));
                }
                puts.add(put);
            }
            this.table.put(puts);

            System.out.println("insert data Success!");
        }else {
            System.out.println("insert data is null !");
        }
    }

    /**
     * 传入表名和rowkwy 进行查询
     * @param tableName
     * @param rowKey
     * @return Student
     */
    public Student getRecordByRowKey(String tableName , String rowKey) throws IOException {
        Table table = this.conn.getTable(TableName.valueOf(tableName));
        Get get = new Get(rowKey.getBytes());
        Student user = new Student();
        user.setStudent_id(rowKey);
        //先判断是否有此条数据
        if(!get.isCheckExistenceOnly()){
            Result result = table.get(get);
            for (Cell cell : result.rawCells()){
                String colName = Bytes.toString(cell.getQualifierArray(),cell.getQualifierOffset(),cell.getQualifierLength());
                String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                if(colName.equals("name")){
                    user.setName(value);
                }
                if(colName.equals("class")){
                    user.setClassInfo(value);
                }
                if (colName.equals("understanding")){
                    user.setUnderstanding(value);
                }
                if (colName.equals("programming")){
                    user.setProgramming(value);
                }
            }
        }
        return user;
    }

    /**
     * 按照RowKey 查询单cell 内容
     * @param tableName
     * @param rowKey
     * @param family
     * @param col
     * @return Cell Data
     */
    public String getCellDataByRowKey(String tableName, String rowKey, String family, String col){
        try {
            Table table = this.conn.getTable(TableName.valueOf(tableName));
            String result = null;
            Get get = new Get(rowKey.getBytes());
            if(!get.isCheckExistenceOnly()){
                get.addColumn(Bytes.toBytes(family),Bytes.toBytes(col));
                Result res = table.get(get);
                byte[] resByte = res.getValue(Bytes.toBytes(family), Bytes.toBytes(col));
                return result = Bytes.toString(resByte);
            }else{
                return result = "查询结果不存在";
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return "Error" ;
    }

    /**
     * 遍历全表
     * @param tableName
     * @return ArrayList<Student>
     */
    public List<Student> queryAllTable(String tableName){
        Table table = null;
        List<Student> list = new ArrayList<Student>();
        try {
            table = this.conn.getTable(TableName.valueOf(tableName));
            ResultScanner results = table.getScanner(new Scan());
            Student user = null;
            for (Result result : results){
                String id = new String(result.getRow());
                user = new Student();
                for(Cell cell : result.rawCells()){
                    String row = Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
                    String colName = Bytes.toString(cell.getQualifierArray(),cell.getQualifierOffset(),cell.getQualifierLength());
                    String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                    user.setStudent_id(row);
                    if(colName.equals("name")){
                        user.setName(value);
                    }
                    if(colName.equals("class")){
                        user.setClassInfo(value);
                    }
                    if (colName.equals("understanding")){
                        user.setUnderstanding(value);
                    }
                    if (colName.equals("programming")){
                        user.setProgramming(value);
                    }
                }
                list.add(user);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return list;
    }

    /**
     * 根据表和多个rowkey 批量查询
     * @param tableName
     * @param rowkeyList
     * @return List<String>
     * @throws IOException
     */
    public List<Student> batchQueryTableByRowKey(String tableName , List<String> rowkeyList) throws IOException {
        List<Get> getList = new ArrayList();
        List<Student> list = new ArrayList<Student>();

        this.table = this.conn.getTable( TableName.valueOf(tableName));// 获取表
        for (String rowkey : rowkeyList){//把rowkey加到get里，再把get装到list中
            Get get = new Get(Bytes.toBytes(rowkey));
            getList.add(get);
        }
        Result[] results = this.table.get(getList);//重点在这，直接查getList<Get>
        for (Result result : results){//对返回的结果集进行操作
            Student user = new Student();
            for (Cell cell : result.rawCells()) {
//                String value = Bytes.toString(CellUtil.cloneValue(colName));
                String colName = Bytes.toString(cell.getQualifierArray(),cell.getQualifierOffset(),cell.getQualifierLength());
                String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
//                System.out.println(value);
                if(colName.equals("student_id")){
                    user.setStudent_id(value);
//                    System.out.println("user  = " + user.getStudent_id());
                }
                if(colName.equals("name")){
                    user.setName(value);
                }
                if(colName.equals("class")){
                    user.setClassInfo(value);
                }
                if (colName.equals("understanding")){
                    user.setUnderstanding(value);
                }
                if (colName.equals("programming")){
                    user.setProgramming(value);
                }
            }
            list.add(user);
        }
        return list;
    }

    public void updateCellDataByRowKey(String tableName , String rowKey , String columnFamily , String columnName , String columnValue) throws IOException {
        List<Put> puts = new ArrayList<Put>();
        // 若表不存在，结束
        if (!isExistsTable(tableName))
            return ;
        // Tabel负责记录相关的操作如增删改查等
        this.table = this.conn.getTable(TableName.valueOf(tableName));

        Put put1 = new Put(Bytes.toBytes(rowKey));
        put1.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName), Bytes.toBytes(columnValue));
        this.table.put(put1);
        System.out.println("update cell data success ! ");
    }

    /**
     * 根据rowKey删除一行数据
     *
     * @param tableName
     * @param rowKey
     * @throws IOException
     */
    public void deleteDataByRowKey(String tableName, String rowKey) throws IOException {
        this.table = this.conn.getTable(TableName.valueOf(tableName));
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        this.table.delete(delete);
        System.out.println("delete data by rowkey success !");
    }

    /**
     * 删除某一行的某一个列簇内容
     *
     * @param tableName
     * @param rowKey
     * @param columnFamily
     * @throws IOException
     */
    public void deleteData(String tableName, String rowKey, String columnFamily) throws IOException {
        this.table = this.conn.getTable(TableName.valueOf(tableName));
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        delete.addFamily(Bytes.toBytes(columnFamily));
        this.table.delete(delete);
    }

    /**
     * 删除某一行某个列簇某列的值
     *
     * @param tableName
     * @param rowKey
     * @param columnFamily
     * @param columnName
     * @throws IOException
     */
    public void deleteData(String tableName, String rowKey, String columnFamily, String columnName) throws IOException {
        this.table = this.conn.getTable(TableName.valueOf(tableName));
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        delete.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName));
        this.table.delete(delete);
    }

    /**
     * 删除某一行某个列簇多个列的值
     *
     * @param tableName
     * @param rowKey
     * @param columnFamily
     * @param columnNames
     * @throws IOException
     */
    public void deleteData(String tableName, String rowKey, String columnFamily, List<String> columnNames) throws IOException {
        this.table = this.conn.getTable(TableName.valueOf(tableName));
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        for (String columnName : columnNames) {
            delete.addColumns(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName));
        }
        this.table.delete(delete);
    }

    /**
     * delete hbase table
     * @throws IOException
     */
    @Test
    public void deleteTable() throws IOException {
        TableName tableName = TableName.valueOf(HbaseApiOperation.tableName);
        if(this.admin.tableExists(tableName)){
            // 和hbase shell中 操作步骤一样
            // 先要disable table ， 再去drop
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
        }
    }

    @After
    public void destroy() {
        if (table != null) {
            try {
                table.close();
            } catch (IOException e) {
//                e.printStackTrace();
                System.out.println(e.getMessage());
            }
        }
        if (admin != null ) {
            try {
                admin.close();
            } catch (IOException e) {
//                e.printStackTrace();
                System.out.println(e.getMessage());
            }
        }
        if (conn!=null ){
            try {
                conn.close() ;
            } catch (IOException e) {
//                e.printStackTrace();
                System.out.println(e.getMessage());
            }
        }
    }

    public  ArrayList<Student> generateTestData() throws IOException {
        File file = new File(this.getClass().getResource("/infodata.txt").getFile());
        BufferedReader reader = null;
        ArrayList<Student> arrayList = new ArrayList<Student>() ;
        try {
            reader = new BufferedReader(new FileReader(file));
            String tempStr;
            while ((tempStr = reader.readLine()) != null) {
                String[] data = tempStr.split("\\s+") ;
                Student info = new Student();
                info.setName(data[0]);
                info.setStudent_id(data[1]);
                info.setClassInfo(data[2]);
                info.setUnderstanding(data[3]);
                info.setProgramming(data[4]);
                arrayList.add(info) ;
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e1) {
                    e1.printStackTrace();
                }
            }
        }
        return arrayList ;
    }

    public static void main(String[] args) throws IOException {
        HbaseApiOperation hbaseApiOperation = new HbaseApiOperation();
        // 指定表的多个列簇
        String[] columnFamilys = {"info", "score"};
        // 指定数据VERSIONS
        int versions = 5 ;
        // 生成测试数据
        ArrayList<Student>  studentArrayList = hbaseApiOperation.generateTestData();
        hbaseApiOperation.init();

        // 1. 创建表操作
//        hbaseApiOperation.createTable(HbaseApiOperation.tableName,columnFamilys,versions);
        // 2. 插入数据
//        hbaseApiOperation.insertData(HbaseApiOperation.tableName,studentArrayList);
        // 3. 按照RowKey 点查一行数据
    /*    String rowKey = "G20210579030073" ;
        Student stu = hbaseApiOperation.getRecordByRowKey(HbaseApiOperation.tableName,rowKey);
        System.out.println(stu.getName() + " " + stu.getStudent_id() + " " + stu.getClassInfo()
                + " " + stu.getUnderstanding() + " " + stu.getProgramming());*/
        // 4. 按照RowKey 查询单cell 内容
/*        String rowKey = "G20210579030073" ;
        String coloumnFamily = "info" ;
        String name = hbaseApiOperation.getCellDataByRowKey(HbaseApiOperation.tableName,rowKey,coloumnFamily,"name");
        System.out.println("name = " + name);*/
        // 5. 全表遍历
/*        List<Student> studentList = hbaseApiOperation.queryAllTable(HbaseApiOperation.tableName);
        for (Student stu: studentList) {
            System.out.println(stu.getName() + " " + stu.getStudent_id() + " " + stu.getClassInfo()
                    + " " + stu.getUnderstanding() + " " + stu.getProgramming());
        }*/

        // 6. 根据RowKey update 某一列的值
/*        String rowKey = "G20210579030073" ;
        String coloumnFamily = "info" ;
        hbaseApiOperation.updateCellDataByRowKey(HbaseApiOperation.tableName,rowKey,coloumnFamily,"understanding",String.valueOf(100)) ;*/

        // 7. 根据rowKey删除一行数据
/*        String rowKey = "20210000000004" ;
        hbaseApiOperation.deleteDataByRowKey(HbaseApiOperation.tableName,rowKey);*/

        // 8. 删除某一行 某一列簇的数据
/*        String rowKey = "20210000000003" ;
        String coloumnFamily = "score" ;
        hbaseApiOperation.deleteData(HbaseApiOperation.tableName,rowKey,coloumnFamily);*/

        // 9. 删除某一行某个列簇某列的值
        /*String rowKey = "20210000000002" ;
        String coloumnFamily = "info" ;
        hbaseApiOperation.deleteData(HbaseApiOperation.tableName,rowKey,coloumnFamily,"name");*/

        // 10. 删除某一行某个列簇某几列的值
/*        String rowKey = "20210000000002" ;
        String coloumnFamily = "info" ;
        List<String> list = new ArrayList<String>() ;
        list.add("name") ;
        list.add("class") ;
        hbaseApiOperation.deleteData(HbaseApiOperation.tableName,rowKey,coloumnFamily,list);*/

        // 11. 按照rowkey 批量查询结果
  /*      List<String> list = new ArrayList<String>() ;
        list.add("20210000000002") ;
        list.add("20210000000001") ;
        List<Student> studentList = hbaseApiOperation.batchQueryTableByRowKey(HbaseApiOperation.tableName,list);
        for (Student stu: studentList) {
            System.out.println(stu.getName() + " " + stu.getStudent_id() + " " + stu.getClassInfo()
                    + " " + stu.getUnderstanding() + " " + stu.getProgramming());
        }*/

        // 12. deleteTable
        hbaseApiOperation.deleteTable();

        hbaseApiOperation.destroy();
    }

}
