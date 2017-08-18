package dataguru.kafka.week5;
import org.apache.zookeeper.*;  
import org.apache.zookeeper.data.Stat;  
import java.util.List;  
import java.io.IOException;  
import java.util.Collections;  
import java.util.concurrent.CountDownLatch;  


public class FairElection implements Watcher{  
    private int threadId;  
    private ZooKeeper zk = null;  
    private String selfPath;  
    private String waitPath;  
    private String LOG_PREFIX_OF_THREAD;  
    private static final int SESSION_TIMEOUT = 10000;  
    private static final String GROUP_PATH = "/leader";  
    private static final String SUB_PATH = "/leader/election";  
    private static final String CONNECTION_STRING = "s1:2181,s2:2181,s3:2181";  
      
    private static final int THREAD_NUM = 5;   
    //确保连接zk成功；  
    private CountDownLatch connectedSemaphore = new CountDownLatch(1);  
    //确保所有线程运行结束；  
    private static final CountDownLatch threadSemaphore = new CountDownLatch(THREAD_NUM);  
    public FairElection(int id) {  
        this.threadId = id;  
        LOG_PREFIX_OF_THREAD = threadId+"th competitor  ";  
    }  
    public static void main(String[] args) {  
        for(int i=0; i < THREAD_NUM; i++){  
            final int threadId = i+1;  
            new Thread(){  
                @Override  
                public void run() {  
                    try{  
                        FairElection fe = new FairElection(threadId);  
                        fe.createConnection(CONNECTION_STRING, SESSION_TIMEOUT);  
                        //GROUP_PATH不存在的话，由一个线程创建即可；  
                        synchronized (threadSemaphore){  
                            fe.createPath(GROUP_PATH, "this node is created by the" + threadId + "client", true);  
                        }  
                        fe.elect();  
                    } catch (Exception e){  
                        System.out.println("【The "+threadId+"th client exception");
                        e.printStackTrace();  
                    }  
                }  
            }.start();  
        }  
        try {  
            threadSemaphore.await();  
            System.out.println("all election finished");
        } catch (InterruptedException e) {  
            e.printStackTrace();  
        }  
    }  
// 选举leader
    private void elect() throws KeeperException, InterruptedException {  
        selfPath = zk.create(SUB_PATH,null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);  
        System.out.println(LOG_PREFIX_OF_THREAD+"created election path:"+selfPath);
        if(checkMinPath()){  
            electSuccess();  
        }  
    }  
 
//      创建节点 
  
    public boolean createPath( String path, String data, boolean needWatch) throws KeeperException, InterruptedException {  
        if(zk.exists(path, needWatch)==null){  
            System.out.println( LOG_PREFIX_OF_THREAD + "path created successfully, Path: "  
                    + this.zk.create( path,  
                    data.getBytes(),  
                    ZooDefs.Ids.OPEN_ACL_UNSAFE,  
                    CreateMode.PERSISTENT )  
                    + ", content: " + data );  
        }  
        return true;  
    }  
    /** 
     * 创建ZK连接 
     * @param connectString  ZK服务器地址列表 
     * @param sessionTimeout Session超时时间 
     */  
    public void createConnection( String connectString, int sessionTimeout ) throws IOException, InterruptedException {  
            zk = new ZooKeeper( connectString, sessionTimeout, this);  
            connectedSemaphore.await();  
    }  
// 成功选举为leader 
    public void electSuccess() throws KeeperException, InterruptedException {  
        if(zk.exists(this.selfPath,false) == null){  
            System.out.println(LOG_PREFIX_OF_THREAD+"client no longer exists...");  
            return;  
        }  
        System.out.println(LOG_PREFIX_OF_THREAD + "I'm the leader now");  
        Thread.sleep(2000);  
        System.out.println(LOG_PREFIX_OF_THREAD + "path deleted："+selfPath);  
        zk.delete(this.selfPath, -1);  
        releaseConnection();  
        threadSemaphore.countDown();  
    }  
    /** 
     * 关闭ZK连接 
     */  
    public void releaseConnection() {  
        if ( this.zk !=null ) {  
            try {  
                this.zk.close();  
            } catch ( InterruptedException e ) {}  
        }  
        System.out.println(LOG_PREFIX_OF_THREAD + "release connection");  
    }  
    /** 
     * 检查自己是不是最小的节点 
     * @return 
     */  
    public boolean checkMinPath() throws KeeperException, InterruptedException {  
         List<String> subNodes = zk.getChildren(GROUP_PATH, false);  
         Collections.sort(subNodes);  
         int index = subNodes.indexOf( selfPath.substring(GROUP_PATH.length()+1));  
         switch (index){  
             case -1:{  
                 System.out.println(LOG_PREFIX_OF_THREAD+"node no longer exists..."+selfPath);  
                 return false;  
             }  
             case 0:{  
                 System.out.println(LOG_PREFIX_OF_THREAD+"I'm the leader among all nodes"+selfPath);  
                 return true;  
             }  
             default:{  
                 this.waitPath = GROUP_PATH +"/"+ subNodes.get(index - 1);  
                 System.out.println(LOG_PREFIX_OF_THREAD+"watch the node in front of me"+waitPath);  
                 try{  
                     zk.getData(waitPath, true, new Stat());  
                     return false;  
                 }catch(KeeperException e){  
                     if(zk.exists(waitPath,false) == null){  
                         System.out.println(LOG_PREFIX_OF_THREAD+"the node in front of me"+waitPath+"has disappeared");  
                         return checkMinPath();  
                     }else{  
                         throw e;  
                     }  
                 }  
             }  
                   
         }  
       
    }  
    @Override  
    public void process(WatchedEvent event) {  
        if(event == null){  
            return;  
        }  
        Event.KeeperState keeperState = event.getState();  
        Event.EventType eventType = event.getType();  
        if ( Event.KeeperState.SyncConnected == keeperState) {  
            if ( Event.EventType.None == eventType ) {  
                System.out.println( LOG_PREFIX_OF_THREAD + "successfully connected to zk cluster" );  
                connectedSemaphore.countDown();  
            }else if (event.getType() == Event.EventType.NodeDeleted && event.getPath().equals(waitPath)) {  
                System.out.println(LOG_PREFIX_OF_THREAD + "the node in front of me has been deleted,I shall be the leader");  
                try {  
                    if(checkMinPath()){  
                        electSuccess();  
                    }  
                } catch (KeeperException e) {  
                    e.printStackTrace();  
                } catch (InterruptedException e) {  
                    e.printStackTrace();  
                }  
            }  
        }else if ( Event.KeeperState.Disconnected == keeperState ) {  
            System.out.println( LOG_PREFIX_OF_THREAD + "connection to zk closed" );  
        } else if ( Event.KeeperState.AuthFailed == keeperState ) {  
            System.out.println( LOG_PREFIX_OF_THREAD + "failed authentication" );  
        } else if ( Event.KeeperState.Expired == keeperState ) {  
            System.out.println( LOG_PREFIX_OF_THREAD + "failed session" );  
        }  
    }  
}  