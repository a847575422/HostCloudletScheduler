package org.cloudbus.cloudsim;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import org.cloudbus.cloudsim.core.CloudSim;

public class HCloudletSchedulerSpaceShared extends CloudletSchedulerSpaceShared {

	private double averageWaitingTime; // 平均等待时间
	private Queue<HResCloudlet> cloudletWaitingQueue; // 每个Vm的等待任务队列
	private int cloudletWaitingQueueLength; // 等待队列长度

	public HCloudletSchedulerSpaceShared(int maxLength) {
		super();
		setAverageWaitingTime(0);
		cloudletWaitingQueue = new LinkedList<HResCloudlet>();
		setCloudletWaitingQueueLength(maxLength);
	}

	@Override
	public double updateVmProcessing(double currentTime, List<Double> mipsShare) {
		setCurrentMipsShare(mipsShare);

		double timeSpam = currentTime - getPreviousTime();															
		double capacity = getCapacity(mipsShare);

		// each machine in the exec list has the same amount of cpu
		for (ResCloudlet rcl : getCloudletExecList()) {
			rcl.updateCloudletFinishedSoFar((long) (capacity * timeSpam
					* rcl.getNumberOfPes() * Consts.MILLION));
		}

		// update each cloudlet
		int finished = 0;
		List<ResCloudlet> toRemove = new ArrayList<ResCloudlet>();
		for (ResCloudlet rcl : getCloudletExecList()) {
			// finished anyway, rounding issue...
			if (rcl.getRemainingCloudletLength() == 0) {
				toRemove.add(rcl);
				cloudletFinish(rcl);
				finished++;
			}
		}
		getCloudletExecList().removeAll(toRemove);
		
		return (double)finished;

	}

	private double getCapacity(List<Double> mipsShare){
		double capacity = 0.0;
		int cpus = 0;

		for (Double mips : mipsShare) { // count the CPUs available to the VMM
			capacity += mips;
			if (mips > 0) {
				cpus++;
			}
		}
		currentCpus = cpus;
		return capacity /= cpus;
	}
	
	public double nextEstimatedFinishTime(double currentTime, List<Double> mipsShare){
		double nextEvent = Double.MAX_VALUE;
		for (ResCloudlet rcl : getCloudletExecList()) {
			double remainingLength = rcl.getRemainingCloudletLength();
			double estimatedFinishTime = currentTime
					+ (remainingLength / (getCapacity(mipsShare) * rcl.getNumberOfPes()));
			if (estimatedFinishTime - currentTime < CloudSim
					.getMinTimeBetweenEvents()) {
				estimatedFinishTime = currentTime
						+ CloudSim.getMinTimeBetweenEvents();
			}
			if (estimatedFinishTime < nextEvent) {
				nextEvent = estimatedFinishTime;
			}
		}
		setPreviousTime(currentTime);
		return nextEvent;
	}
	
	@Override
	public double cloudletSubmit(Cloudlet cloudlet, double fileTransferTime) {
		// it can go to the exec list
		if ((currentCpus - usedPes) >= cloudlet.getNumberOfPes()) {
			HResCloudlet rcl = new HResCloudlet(cloudlet,
					cloudlet.getExecStartTime());
			updateAverageWaitingTime(cloudlet.getWaitingTime());// 更新平均等待时间
			// Log.printLine("VM # "+cloudlet.getVmId()+" waitingTime: "+getAverageWaitingTime());
			rcl.setCloudletStatus(Cloudlet.INEXEC);
			for (int i = 0; i < cloudlet.getNumberOfPes(); i++) {
				rcl.setMachineAndPeId(0, i);
			}
			getCloudletExecList().add(rcl);
			usedPes += cloudlet.getNumberOfPes();
		} else {// no enough free PEs: go to the waiting queue
			HResCloudlet rcl = new HResCloudlet(cloudlet,
					cloudlet.getExecStartTime());
			rcl.setCloudletStatus(Cloudlet.QUEUED);
			if (addWaitingCloudlet(rcl))
				return 0.0;
			else
				return -1.0;
		}

		// calculate the expected time for cloudlet completion
		double capacity = 0.0;
		int cpus = 0;
		for (Double mips : getCurrentMipsShare()) {
			capacity += mips;
			if (mips > 0) {
				cpus++;
			}
		}

		currentCpus = cpus;
		capacity /= cpus;

		// use the current capacity to estimate the extra amount of
		// time to file transferring. It must be added to the cloudlet length
		double extraSize = capacity * fileTransferTime;
		long length = cloudlet.getCloudletLength();
		length += extraSize;
		cloudlet.setCloudletLength(length);
		return cloudlet.getCloudletLength() / capacity;
	}

	public boolean addWaitingCloudlet(HResCloudlet cloudlet) {
		if (cloudletWaitingQueue.size() < getCloudletWaitingQueueLength())
			return cloudletWaitingQueue.offer(cloudlet);
		else {
			Log.printLine("ERROR:VM #" + cloudlet.getCloudlet().getVmId()
					+ " add Cloudlet #" + cloudlet.getCloudletId()
					+ " FAILDED!! Queue Size :" + cloudletWaitingQueue.size());
			return false;
		}
	}

	public HResCloudlet removeWaitingCloudlet() {
		return cloudletWaitingQueue.poll();
	}

	private void updateAverageWaitingTime(double newWaitingTime) {
		setAverageWaitingTime((getAverageWaitingTime()
				* getCloudletFinishedList().size() + newWaitingTime)
				/ (getCloudletFinishedList().size() + 1));
	}

	
	public int getCurrentCpus(){
		return currentCpus;
	}
	
	public int getUsedPes(){
		return usedPes;
	}
	
	public void setUsedPes(int usedPes){
		 super.usedPes = usedPes;
	}
	
	public double getAverageWaitingTime() {
		return averageWaitingTime;
	}

	public void setAverageWaitingTime(double averageWaitingTime) {
		this.averageWaitingTime = averageWaitingTime;
	}

	public Queue<HResCloudlet> getCloudletWaitingQueue() {
		return cloudletWaitingQueue;
	}

	public int getCloudletWaitingQueueLength() {
		return cloudletWaitingQueueLength;
	}

	public void setCloudletWaitingQueueLength(int cloudletWaitingQueueLength) {
		this.cloudletWaitingQueueLength = cloudletWaitingQueueLength;
	}
	
}
