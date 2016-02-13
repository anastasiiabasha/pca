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
	
}
