package collect;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.net.InetSocketAddress;
import java.util.List;

public class CanalTest {
    public static void main(String[] args) {

        //连接Canal
        CanalConnector canalConnector = CanalConnectors.newSingleConnector(new InetSocketAddress("10.10.150.10", 11111), "zhanghd", "", "");

        //
        while (true) {
            canalConnector.connect();
            canalConnector.subscribe("gmall2021.*");

            Message message = canalConnector.get(100);

            if (message.getEntries().size() <= 0) {
                System.out.println("没有数据，休息一会！！！");
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            } else {
                for (CanalEntry.Entry entry : message.getEntries()) {

                    if (CanalEntry.EntryType.ROWDATA.equals(entry.getEntryType())) {

                        try {
                            //1.获取表名
                            String tableName = entry.getHeader().getTableName();
                            //2.获取数据
                            ByteString storeValue = entry.getStoreValue();
                            CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(storeValue);

                            //3.获取数据
                            List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();

                            //4.获取数据操作类型
                            CanalEntry.EventType eventType = rowChange.getEventType();

                            //5.根据不同的表,处理数据
                            //handle(tableName, eventType, rowDatasList);

                        } catch (InvalidProtocolBufferException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
        }
    }


}

