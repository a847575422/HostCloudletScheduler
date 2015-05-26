package org.cloudbus.cloudsim.main;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.DatacenterCharacteristics;
import org.cloudbus.cloudsim.Host;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.Pe;
import org.cloudbus.cloudsim.Storage;
import org.cloudbus.cloudsim.VmAllocationPolicySimple;
import org.cloudbus.cloudsim.VmSchedulerTimeShared;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.provisioners.BwProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.PeProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.RamProvisionerSimple;

public class MainExample {

	private static final int NUM_HOST = 10;
	private static final int HOST_CLOUDLET_WAITING_LENGTH = 50;
	private static final int NUM_VM = 4;

	private static final int NUM_CLOUDLET = 1000;
	private static final double POISSON_LAMBDA = 10.0;
	private static final double LETS_WAVE_INTERVAL = 200.0;
//	private static final int MAX_LENGTH_WAITING_QUEUE = 50;
	
	public static void main(String[] args) {
		Log.printLine("Starting...");

		try {
			int num_user = 1;
			Calendar calendar = Calendar.getInstance();
			boolean trace_flag = false;

			CloudSim.init(num_user, calendar, trace_flag);

			int numHost = 1;
			int numVm = NUM_VM;
			QDatacenter datacenter0 = createDatacenter("Datacenter_0", numHost, numVm);

			VmCloudletAssigner vmCloudletAssigner = new VmCloudletAssignerRandom();
			int numlets = NUM_CLOUDLET;
			int numPe = NUM_VM;
			double lambda = POISSON_LAMBDA;
			double numletWaveInterval = LETS_WAVE_INTERVAL;
			int cloudletWaitingQueueLength = HOST_CLOUDLET_WAITING_LENGTH;
			QDatacenterBroker globalBroker = new QDatacenterBroker("QDatacenterBroker",vmCloudletAssigner, numlets, numPe, 
					lambda, numletWaveInterval, cloudletWaitingQueueLength);

			CloudSim.startSimulation();

			List<Cloudlet> newList = new LinkedList<Cloudlet>();
			HashMap<Integer, Double> waitingTimeList = new HashMap<Integer, Double>();
//			int numVm = datacenter0.getVmList().size();

			newList.addAll(globalBroker.getCloudletReceivedList());
			
//			for (int i = 0; i < numVm; i++) {
//				waitingTimeList.put(datacenter0.getVmList().get(i).getId(), 
//						((QCloudletSchedulerSpaceShared) datacenter0.getVmList()
//						.get(i).getCloudletScheduler()).getAverageWaitingTime());
//			}

			CloudSim.stopSimulation();
			Log.printLine("Total Cloudlets: "+getTotalNumOfCloudlets(globalBroker));
			printCloudletList(newList);

//			System.out.println("以下是每个虚拟机的平均等待时间：");
//			for (int i = 0; i < numVm; i++) {
//				System.out.println("Vm#" + i + ": " + waitingTimeList.get(i));
//			}

			Log.printLine("finished!");
		}
		catch (Exception e)
		{
			e.printStackTrace();
			Log.printLine("The simulation has been terminated due to an unexpected error");
		}
	}
	
	private static QDatacenter createDatacenter(String name, int numHost, int numPe){

		List<Host> hostList = new ArrayList<Host>();
		List<Pe> peList = new ArrayList<Pe>();

		int mips = 1000;

		for (int i = 0; i < numPe; i++) {
			peList.add(new Pe(i, new PeProvisionerSimple(mips)));
		}

		int hostId=0;
		int ram = 16384;
		long storage = 1000000;
		int bw = 10000;
		
		for (int i = 0; i < numHost; i++) {
			hostList.add(new Host(
					hostId,
					new RamProvisionerSimple(ram),
					new BwProvisionerSimple(bw),
					storage,
					peList,
					new VmSchedulerTimeShared(peList)
				));
	
			hostId++;
		}

		String arch = "x86";      // system architecture
		String os = "Linux";          // operating system
		String vmm = "Xen";
		double time_zone = 10.0;         // time zone this resource located
		double cost = 3.0;              // the cost of using processing in this resource
		double costPerMem = 0.05;		// the cost of using memory in this resource
		double costPerStorage = 0.1;	// the cost of using storage in this resource
		double costPerBw = 0.1;			// the cost of using bw in this resource
		LinkedList<Storage> storageList = new LinkedList<Storage>();	//we are not adding SAN devices by now

		DatacenterCharacteristics characteristics = new DatacenterCharacteristics(
                arch, os, vmm, hostList, time_zone, cost, costPerMem, costPerStorage, costPerBw);

		QDatacenter datacenter = null;
		try {
			datacenter = new QDatacenter(name, characteristics, new VmAllocationPolicySimple(hostList), storageList, 0);
		} catch (Exception e) {
			e.printStackTrace();
		}

		return datacenter;
	}
	
	/**
	 * Prints the Cloudlet objects
	 * @param list  list of Cloudlets
	 */
	private static void printCloudletList(List<Cloudlet> list) {
		int size = list.size();
		Cloudlet cloudlet;

		String indent = "    ";
		int success = 0;
		Log.printLine();
		Log.printLine("========== OUTPUT ==========");
		Log.printLine("Cloudlet ID" + indent + "STATUS" + indent +
				"Data center ID" + indent + "VM ID" + indent + indent + "Time" + 
				indent + "Submission Time" + indent + "Start Time" + indent + "Finish Time");

		DecimalFormat dft = new DecimalFormat("###.##");
		for (int i = 0; i < size; i++) {
			cloudlet = list.get(i);
			//Log.print(indent + cloudlet.getCloudletId() + indent + indent);

			if (cloudlet.getCloudletStatus() == Cloudlet.SUCCESS){
				success++;
				/*Log.print("SUCCESS");

				Log.printLine( indent + indent + cloudlet.getResourceId() + indent + indent + indent + cloudlet.getVmId() +
						indent + indent + indent + dft.format(cloudlet.getActualCPUTime()) + 
						indent + indent + indent + dft.format(cloudlet.getSubmissionTime()) +
						indent + indent + dft.format(cloudlet.getExecStartTime()-cloudlet.getSubmissionTime())+ indent + indent + indent + 
						dft.format(cloudlet.getFinishTime()));*/
			}
		}

		Log.printLine("Number of Success cloudlet : "+success);
	}

}
