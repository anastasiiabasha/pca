package flink.pca.impl.svd;

import java.io.Serializable;

public class ArpackContext implements Serializable {
	
	private int              ido;
	private double           tol;
	private double[]         resid;
	private double[]         v;
	private int[]            iparam;
	private int[]            ipntr;
	private int              info;
	private double[]         workd;
	private double[]         workl;
	
	
	public ArpackContext(int ido, double tol, double[] resid, double[] v, int[] iparam, int[] ipntr, int info, double[] workd, double[] workl) {
		this.setIdo(ido);
		this.setTol(tol);
		this.setResid(resid);
		this.setV(v);
		this.setIparam(iparam);
		this.setIpntr(ipntr);
		this.setInfo(info);
		this.setWorkd(workd);
		this.setWorkl(workl);
	}

	public int getIdo() {
		return ido;
	}

	public void setIdo(int ido) {
		this.ido = ido;
	}
	
	public double getTol() {
		return tol; 
	}
	
	public void setTol(double tol) {
		this.tol = tol;
	}

	public double[] getResid() {
		return resid;
	}

	public void setResid(double[] resid) {
		this.resid = resid;
	}

	public double[] getV() {
		return v;
	}

	public void setV(double[] v) {
		this.v = v;
	}

	public int[] getIparam() {
		return iparam;
	}

	public void setIparam(int[] iparam) {
		this.iparam = iparam;
	}

	public int[] getIpntr() {
		return ipntr;
	}

	public void setIpntr(int[] ipntr) {
		this.ipntr = ipntr;
	}

	public int getInfo() {
		return info;
	}

	public void setInfo(int info) {
		this.info = info;
	}

	public double[] getWorkd() {
		return workd;
	}

	public void setWorkd(double[] workd) {
		this.workd = workd;
	}

	public double[] getWorkl() {
		return workl;
	}

	public void setWorkl(double[] workl) {
		this.workl = workl;
	}
	
	public ArpackContext copy() {
		double[] oldResid = new double[resid.length];
		System.arraycopy(resid, 0, oldResid, 0, resid.length);
		double[] oldV = new double[v.length];
		System.arraycopy(v, 0, oldV, 0, v.length);
		
		int[] oldIparam = new int[iparam.length];
		System.arraycopy(iparam, 0, oldIparam, 0, iparam.length);
		int[] oldIpntr = new int[ipntr.length];
		System.arraycopy(ipntr, 0, oldIpntr, 0, ipntr.length);
		double[] oldWorkd = new double[workd.length];
		System.arraycopy(workd, 0, oldWorkd, 0, workd.length);
		double[] oldWorkl = new double[workl.length];
		System.arraycopy(workl, 0, oldWorkl, 0, workl.length);
		
		return new ArpackContext(ido, tol, oldResid, oldV, oldIparam, oldIpntr, info, oldWorkd, oldWorkl);
	}
	
}
