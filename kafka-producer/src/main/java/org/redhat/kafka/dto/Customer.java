package org.redhat.kafka.dto;

public class Customer {
	private String sNo;
	private String reportingEntityId;
	private String contractId;
	private String pan;
	private String gstin;
	private String name;
	private String dateofBirth;
	private String mobNumber;
	private String email;
	private String creditRating;
	private String custData;
	private Long kafkaKey;
	private double interestRate;
	
	
	private String errorStatus;


	public String getsNo() {
		return sNo;
	}


	public void setsNo(String sNo) {
		this.sNo = sNo;
	}


	public String getReportingEntityId() {
		return reportingEntityId;
	}


	public void setReportingEntityId(String reportingEntityId) {
		this.reportingEntityId = reportingEntityId;
	}


	public String getContractId() {
		return contractId;
	}


	public void setContractId(String contractId) {
		this.contractId = contractId;
	}


	public String getPan() {
		return pan;
	}


	public void setPan(String pan) {
		this.pan = pan;
	}


	public String getGstin() {
		return gstin;
	}


	public void setGstin(String gstin) {
		this.gstin = gstin;
	}


	public String getName() {
		return name;
	}


	public void setName(String name) {
		this.name = name;
	}


	public String getDateofBirth() {
		return dateofBirth;
	}


	public void setDateofBirth(String dateofBirth) {
		this.dateofBirth = dateofBirth;
	}


	public String getMobNumber() {
		return mobNumber;
	}


	public void setMobNumber(String mobNumber) {
		this.mobNumber = mobNumber;
	}


	public String getEmail() {
		return email;
	}


	public void setEmail(String email) {
		this.email = email;
	}


	public String getCreditRating() {
		return creditRating;
	}


	public void setCreditRating(String creditRating) {
		this.creditRating = creditRating;
	}


	public String getCustData() {
		return custData;
	}


	public void setCustData(String custData) {
		this.custData = custData;
	}


	public Long getKafkaKey() {
		return kafkaKey;
	}


	public void setKafkaKey(Long kafkaKey) {
		this.kafkaKey = kafkaKey;
	}


	public String getErrorStatus() {
		return errorStatus;
	}


	public void setErrorStatus(String errorStatus) {
		this.errorStatus = errorStatus;
	}


	public double getInterestRate() {
		return interestRate;
	}


	public void setInterestRate(double interestRate) {
		this.interestRate = interestRate;
	}
	
	@Override
	public String toString() {
		return "name="+this.name+" pancardNo="+this.pan+"reportingEntityId="+this.reportingEntityId+"contractId="+this.contractId+"gstin="+this.gstin+" dob="+this.dateofBirth+" interestRate="+this.interestRate+"mobNumber="+this.mobNumber+" email="+this.email+"creditRating="+this.creditRating+"custData="+this.custData+ "errorStatus="+this.errorStatus;
	}

	
}
