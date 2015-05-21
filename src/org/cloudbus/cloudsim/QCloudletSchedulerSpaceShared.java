package org.cloudbus.cloudsim;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import org.cloudbus.cloudsim.core.CloudSim;

public class QCloudletSchedulerSpaceShared extends CloudletSchedulerSpaceShared {

	private double averageWaitingTime; // 平均等待时间
	private Queue<ResCloudlet> cloudletWaitingQueue;
	private int cloudletWaitingQueueLength; // 等待队列长度
	private List<ResCloudlet> decardedCloudletList;

	public QCloudletSchedulerSpaceShared() {
		super();
		cloudletWaitingQueue = new LinkedList<ResCloudlet>();
		decardedCloudletList = new ArrayList<ResCloudlet>();
	}

	@Override
	public double updateVmProcessing(double currentTime, List<Double> mipsShare) {
		setCurrentMipsShare(mipsShare);

		double timeSpam = currentTime - getPreviousTime(); // time since last
															// update
		double capacity = 0.0;
		int cpus = 0;

		for (Double mips : mipsShare) { // count the CPUs available to the VMM
			capacity += mips;
			if (mips > 0) {
				cpus++;
			}
		}
		currentCpus = cpus;
		capacity /= cpus; // average capacity of each cpu

		// each machine in the exec list has the same amount of cpu
		for (ResCloudlet rcl : getCloudletExecList()) {
			rcl.updateCloudletFinishedSoFar((long) (capacity * timeSpam
					* rcl.getNumberOfPes() * Consts.MILLION));
		}

		// no more cloudlets in this scheduler
		if (getCloudletExecList().size() == 0
				&& getCloudletWaitingQueue().size() == 0) {
			setPreviousTime(currentTime);
			return 0.0;
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

		// for each finished cloudlet, add a new one from the waiting list
		if (!getCloudletWaitingQueue().isEmpty()) {
			for (int i = 0; i < finished; i++) {
				toRemove.clear();
				for (ResCloudlet rcl : getCloudletWaitingList()) {
					if ((currentCpus - usedPes) >= rcl.getNumberOfPes()) { //注：这里的任务都是单核的，代码才兼容。
						rcl.setCloudletStatus(Cloudlet.INEXEC);
						for (int k = 0; k < rcl.getNumberOfPes(); k++) {
							rcl.setMachineAndPeId(0, k);		//注：cloudsim源码有错！
						}
						getCloudletExecList().add(rcl);
						usedPes += rcl.getNumberOfPes();
						getCloudletWaitingQueue().poll();
						//toRemove.add(rcl);
						break;
					}
				}
				//getCloudletWaitingList().removeAll(toRemove);
			}
		}

		// estimate finish time of cloudlets in the execution queue
		double nextEvent = Double.MAX_VALUE;
		for (ResCloudlet rcl : getCloudletExecList()) {
			double remainingLength = rcl.getRemainingCloudletLength();
			double estimatedFinishTime = currentTime
					+ (remainingLength / (capacity * rcl.getNumberOfPes()));
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
			ResCloudlet rcl = new ResCloudlet(cloudlet);
			rcl.setCloudletStatus(Cloudlet.INEXEC);
			for (int i = 0; i < cloudlet.getNumberOfPes(); i++) {
				rcl.setMachineAndPeId(0, i);
			}
			getCloudletExecList().add(rcl);
			usedPes += cloudlet.getNumberOfPes();
		} else {// no enough free PEs: go to the waiting queue
			ResCloudlet rcl = new ResCloudlet(cloudlet);
			rcl.setCloudletStatus(Cloudlet.QUEUED);
			getCloudletWaitingList().add(rcl);
			return 0.0;
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
	
	
	public boolean addCloudlet(ResCloudlet cloudlet) {
		if (cloudletWaitingQueue.size() < getCloudletWaitingQueueLength())
			return cloudletWaitingQueue.offer(cloudlet);
		else
			return false;
	}

	public ResCloudlet removeCloudlet() {
		return cloudletWaitingQueue.poll();
	}

	private void updateAverageWaitingTime(double newWaitingTime) {
		setAverageWaitingTime((getAverageWaitingTime()
				* getCloudletFinishedList().size() + newWaitingTime)
				/ (getCloudletFinishedList().size() + 1));
	}

	public double getAverageWaitingTime() {
		return averageWaitingTime;
	}

	public void setAverageWaitingTime(double averageWaitingTime) {
		this.averageWaitingTime = averageWaitingTime;
	}
	
	public Queue<ResCloudlet> getCloudletWaitingQueue() {
		return cloudletWaitingQueue;
	}

	public int getCloudletWaitingQueueLength() {
		return cloudletWaitingQueueLength;
	}

	public void setCloudletWaitingQueueLength(int cloudletWaitingQueueLength) {
		this.cloudletWaitingQueueLength = cloudletWaitingQueueLength;
	}

	public List<ResCloudlet> getDecardedCloudletList() {
		return decardedCloudletList;
	}

	public void setDecardedCloudletList(List<ResCloudlet> decardedCloudletList) {
		this.decardedCloudletList = decardedCloudletList;
	}

}
