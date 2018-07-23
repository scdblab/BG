/**
 * Authors:  Aniruddh Munde and Snehal Deshmukh
 */
package edu.usc.bg;

public class Times {

	/**
	 * @param args
	 */
	Double timeAtCreation;
	Double timeBeforeService;
	Double timeAfterService;
	Double ClientQueueingTime;
	Times()
	{timeAtCreation=new Double(0.0);
		 timeBeforeService=new Double(0.0);
		 timeAfterService=new Double(0.0);
		 ClientQueueingTime=new Double(0.0);
	}
	public Times(double d) {
		timeAtCreation=new Double(d);
		timeBeforeService=new Double(0.0);
		 timeAfterService=new Double(0.0);
		 ClientQueueingTime=new Double(0.0);
	}
	public void setTimeBeforeService(double before)
	{
		 this.timeBeforeService=before;
	}
	public void setTimeAtCreation(double creationTime)
	{
		 this.timeAtCreation=creationTime;
	}
	public void setTimeAfterService(double after)
	{
		 this.timeAfterService=after;
	}
	public void setClientQueueingTime(double clientq)
	{
		 this.ClientQueueingTime=clientq;
	}
	public double getTimeBeforeService()
	{
		 return this.timeBeforeService.doubleValue();
	}
	public double getTimeAtCreation()
	{
		 return this.timeAtCreation.doubleValue();
	}
	public double getTimeAfterService()
	{
		 return this.timeAfterService.doubleValue();
	}
	public double getClientQueueingTime()
	{
		 return this.ClientQueueingTime.doubleValue();
	}
}
