package edu.usc.bg.server;

import java.util.Iterator;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import edu.usc.bg.base.DB;
import edu.usc.bg.base.DBException;
import edu.usc.bg.base.DBFactory;
import edu.usc.bg.base.UnknownDBException;


public class DBPool {
	public Semaphore poolSemaphore;
    public ConcurrentHashMap<DB,Long> availPool;
    private long maxWaiting=0;
    
//    public ConcurrentHashMap<DB,Long> busyPool;
    private int initConn = 10;
    
    public  DBPool(String dbname, Properties properties, int con)
    {
    	initConn=con;
    	 if(null == availPool)
         	availPool = new ConcurrentHashMap<DB,Long>( initConn );
//         if(null == busyPool)
//         	busyPool = new ConcurrentHashMap<DB,Long>( initConn);
         poolSemaphore= new Semaphore (0,true);
         for (int j = 0; j < initConn; j++) 
         {
        	DB db=null;
			try {
				db = DBFactory.newDB(dbname, properties);
				db.init();
			} catch (DBException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (UnknownDBException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
				
         addDBToPool(availPool, db);
			poolSemaphore.release();
         }
    	
    }
public void shutdownPool()
{
	if (availPool.size()!=initConn)
	{
		System.out.println("DB pool size not match the number of DB connections");
		System.exit(0);
	}
	Iterator<DB> i = availPool.keySet().iterator();
	while(i.hasNext())
	{
		try {
			i.next().cleanup(false);
		} catch (DBException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		i.remove();
	}
	availPool=null;
	poolSemaphore=null;
	
	
	
}
	private void addDBToPool(ConcurrentHashMap<DB, Long> pool, DB db) {
		// TODO Auto-generated method stub
		pool.put(db, new Long(System.currentTimeMillis()));
		
	}
	public DB getConnection()
	{
		
		try {
			if(poolSemaphore.tryAcquire(0,TimeUnit.SECONDS)==false)
				
			{
				long numWaiting=poolSemaphore.getQueueLength();
				setMaxWaiting(Math.max(getMaxWaiting(), numWaiting));
				
				
//				System.out.println("Not able to get DB immediatly...");
//				System.exit(0);
				poolSemaphore.acquire();
			}
		
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		if (availPool==null || availPool.isEmpty())
		{
			System.out.println("DB pool not initialized or not sync with semaphore");
			System.exit(0);
		}
		
		Iterator<DB> i=null; 
		DB db=null;
		synchronized (availPool) {
		i= availPool.keySet().iterator();
		db=i.next();
		i.remove();
		}
//		addDBToPool(busyPool, db);
		return db;
		
		
	}
	public void checkIn(DB db)
	{
		addDBToPool(availPool, db);
		poolSemaphore.release();
		
	}
	public long getMaxWaiting() {
		return maxWaiting;
	}
	public void setMaxWaiting(long maxWaiting) {
		this.maxWaiting = maxWaiting;
	}
    
    
    

}
