package com.uk.dpq.driver;

import java.io.FileReader;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.opencsv.CSVReader;

public class UKSponsorsAnalysis {
	public static void main(String[] args) throws InterruptedException {
		for (int i=0 ; i<10 ; i++) {
			System.out.println("Iteration :::::  "+i);
			long startTime = System.currentTimeMillis();
			processWithStream();
			System.out.println("\nTime taken by Stream process::"+ (System.currentTimeMillis() - startTime) );
			Thread.sleep(10);
			startTime = System.currentTimeMillis();
			processWithParallelStream();
			System.out.println("Time taken by parallel Stream process::"+ (System.currentTimeMillis() - startTime) );
		
		}
		System.out.println("Process Completed!!!");
		
	}
	
	static void processWithStream() {
		try {
			// Create an object of filereader
	        // class with CSV file as a parameter.
			FileReader filereader = new FileReader("/Users/dpq/git/CovidDataAnalysis/inputData/2022-01-31_-_Worker_and_Temporary_Worker.csv");
			// create csvReader object passing
	        // file reader as a parameter
			CSVReader csvReader = new CSVReader(filereader);
			//Read all data at once : We read the CSV records one by one using the readNext() method.
			//CSVReader also provides a method called readAll() to read all the records at once into a List.
			List<String[]> allData = csvReader.readAll();
			
			System.out.println("Total count: "+ allData.size());
			
			List<SponsoredDetails> sponsorDetails = new LinkedList<SponsoredDetails>();
			//To fetch all rows
			allData.stream().forEach((c) -> sponsorDetails.add(new SponsoredDetails(c [0], c [1], c [2], c[3])));
			
			// to find range of records 10 to 60
			
			Stream<SponsoredDetails> filteredData = sponsorDetails.stream()
            .skip((long)(allData.size() * 10))
            .limit((long)(allData.size() * (60 - 10)));
			
			System.out.println("range records 10 to 60 which is 50: "+filteredData);
			
			//App 1 is to find weather input company is registered sponsor for UK or not
			String inputCompanyName = "Underground Mining Services Ltd";
			System.out.println("sponsorDetails size:"+sponsorDetails.size());
			System.out.println("IsGivenCountryRegistored: "+ ((List<String>) sponsorDetails.stream().map(SponsoredDetails::getCompanyName)
					.collect(Collectors.toList())).contains(inputCompanyName)
			);
			
			inputCompanyName = "XYZ";
			System.out.println("IsGivenCountryRegistored: "+ ((List<String>) sponsorDetails.stream().map(SponsoredDetails::getCompanyName)
					.collect(Collectors.toList())).contains(inputCompanyName)
			);
		}catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	static void processWithParallelStream() {
		try {
			// Create an object of filereader
	        // class with CSV file as a parameter.
			FileReader filereader = new FileReader("/Users/dpq/git/CovidDataAnalysis/inputData/2022-01-31_-_Worker_and_Temporary_Worker.csv");
			// create csvReader object passing
	        // file reader as a parameter
			CSVReader csvReader = new CSVReader(filereader);
			//Read all data at once : We read the CSV records one by one using the readNext() method.
			//CSVReader also provides a method called readAll() to read all the records at once into a List.
			List<String[]> allData = csvReader.readAll();
			
			System.out.println("Total count: "+ allData.size());
			
			List<SponsoredDetails> sponsorDetails = new LinkedList<SponsoredDetails>();
			//To fetch all rows
			allData.parallelStream().forEach((c) -> sponsorDetails.add(new SponsoredDetails(c [0], c [1], c [2], c[3])));
			
			// to find range of records 10 to 60
			
			Stream<SponsoredDetails> filteredData = sponsorDetails.parallelStream()
		            .skip((long)(allData.size() * 10))
		            .limit((long)(allData.size() * (60 - 10)));
					
					System.out.println("range records 10 to 60 which is 50: "+filteredData);
			
			//App 1 is to find weather input company is registered sponsor for UK or not
			String inputCompanyName = "Underground Mining Services Ltd";
			System.out.println("parallel sponsorDetails size :"+sponsorDetails.size());
			System.out.println("IsGivenCountryRegistored: "+ ((List<String>) sponsorDetails.stream().map(SponsoredDetails::getCompanyName)
					.collect(Collectors.toList())).contains(inputCompanyName)
			);
			
			inputCompanyName = "XYZ";
			System.out.println("IsGivenCountryRegistored: "+ ((List<String>) sponsorDetails.stream().map(SponsoredDetails::getCompanyName)
					.collect(Collectors.toList())).contains(inputCompanyName)
			);
			
		}catch(Exception e) {
			e.printStackTrace();
		}
	}
}

class SponsoredDetails{
	private String companyName;
	private String townOrCityName;
	private String countryName;
	private String rating;
	public SponsoredDetails(String companyName, String townOrCityName, String countryName, String rating) {
		super();
		this.companyName = companyName;
		this.townOrCityName = townOrCityName;
		this.countryName = countryName;
		this.rating = rating;
	}
	public String getCompanyName() {
		return companyName;
	}
	public String getTownOrCityName() {
		return townOrCityName;
	}
	public String getCountryName() {
		return countryName;
	}
	public String getRating() {
		return rating;
	}
	@Override
	public String toString() {
		return "SponsoredDetails [companyName=" + companyName + ", townOrCityName=" + townOrCityName + ", countryName="
				+ countryName + ", rating=" + rating + "]";
	}
	
	
}
