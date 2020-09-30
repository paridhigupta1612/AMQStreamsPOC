package org.redhat.kafka.dto;

public class Customer {

	private String name;
	private String pancardNo;
	private String dob;
	private double amount;
	private String loanAccNo;
	private String lastPaymentDate;
	private double outstandingAmount;
	private boolean isValidationFailed;
	
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getPancardNo() {
		return pancardNo;
	}
	public void setPancardNo(String pancardNo) {
		this.pancardNo = pancardNo;
	}
	public String getDob() {
		return dob;
	}
	public void setDob(String dob) {
		this.dob = dob;
	}
	public double getAmount() {
		return amount;
	}
	public void setAmount(double amount) {
		this.amount = amount;
	}
	public String getLoanAccNo() {
		return loanAccNo;
	}
	public void setLoanAccNo(String loanAccNo) {
		this.loanAccNo = loanAccNo;
	}
	public String getLastPaymentDate() {
		return lastPaymentDate;
	}
	public void setLastPaymentDate(String lastPaymentDate) {
		this.lastPaymentDate = lastPaymentDate;
	}
	public double getOutstandingAmount() {
		return outstandingAmount;
	}
	public void setOutstandingAmount(double outstandingAmount) {
		this.outstandingAmount = outstandingAmount;
	}
	
	public boolean isValidationFailed() {
		return isValidationFailed;
	}
	public void setValidationFailed(boolean isValidationFailed) {
		this.isValidationFailed = isValidationFailed;
	}
	@Override
	public String toString() {
		return "name="+this.name+" pancardNo="+this.pancardNo+" dob="+this.dob+" amount="+this.amount+" loanAccNo="+this.loanAccNo+" lastPaymentDate="+this.lastPaymentDate+ " outstandingAmount="+this.outstandingAmount;
	}
	
}