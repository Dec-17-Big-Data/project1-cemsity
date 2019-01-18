package com.revature.question4;

public class YearData {
	private int year;
	private double data;
	
	public YearData(String string) {
		this.fromString(string);
	}
	
	public YearData(int year, double data) {
		this.year = year;
		this.data = data;
	}

	public int getYear() {
		return year;
	}

	public void setYear(int year) {
		this.year = year;
	}

	public double getData() {
		return data;
	}

	public void setData(double data) {
		this.data = data;
	}
	public String toString() {
		return "" + this.year + "%%" + this.data;
	}
	
	public void fromString(String string) {
		String[] arr = string.split("%%");
		this.year = Integer.parseInt(arr[0]);
		this.data = Double.parseDouble(arr[0]);		
	}
}
