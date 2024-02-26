package com.hunter.hunter;

public class SendEmail {

	public static void main(String[] args) {
		Email email=new Email("ranjan.kumar2@shubham.co,vijay.uniyal@shubham.co", "Testing", "Testing",null, "smtp.shubham.co", "465", "no-reply.huntermis@shubham.co", "Skittles@109");
		Thread emailThread = new Thread(email);
		emailThread.start();
		
	}
}