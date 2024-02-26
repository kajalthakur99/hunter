package com.hunter.hunter;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

import javax.mail.MessagingException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

@Configuration
@EnableScheduling
public class RunScheduler {

	@Autowired
	@Qualifier("jdbcTemplate2")
	private JdbcTemplate osourceTemplate;

	// @Scheduled(cron = "0 0/10 * * * ?")

	// @Scheduled(cron = "0 30 11 * * ?")
   //  @Scheduled(cron = "0 4 16 * * ?")
	@Scheduled(cron = "0 0 9,16 * * ?")
	//@Scheduled(cron = "0 51 13 * * ?")
	public void scheduleFixedDelayTask() throws MessagingException {

		try {
			sendemail("scheduleFixedDelayTask--->fetchData");
			URL url = new URL("http://localhost:8080/fetchData");
			HttpURLConnection con = (HttpURLConnection) url.openConnection();
			con.setRequestMethod("GET");

			BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
			String inputLine;
			while ((inputLine = in.readLine()) != null) {
				System.out.println(inputLine);
			}

			in.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	
	
	//@Scheduled(cron = "0 59 12 * * ?")
	public void schedulefetchDataPerNeed() throws MessagingException {

		try {
			//sendemail("scheduleFixedDelayTask--->fetchData");
			URL url = new URL("http://localhost:8080/fetchDataPerNeed");
			HttpURLConnection con = (HttpURLConnection) url.openConnection();
			con.setRequestMethod("GET");

			BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
			String inputLine;
			while ((inputLine = in.readLine()) != null) {
				System.out.println(inputLine);
			}

			in.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	
	@Scheduled(cron = "0 0 9,16 * * ?")
	//@Scheduled(cron = "0 56 13 * * ?")
	//@Scheduled(cron = "0 40 19 * * ?")
	public void scheduleFixedDelayTaskSME() throws MessagingException {

		try {
			sendemail("scheduleFixedDelayTaskSME-->fetchSMEData");
			URL url = new URL("http://localhost:8080/fetchSMEData");
			HttpURLConnection con = (HttpURLConnection) url.openConnection();
			con.setRequestMethod("GET");

			BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
			String inputLine;
			while ((inputLine = in.readLine()) != null) {
				System.out.println(inputLine);
			}

			in.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	//@Scheduled(cron = "0 30 09 * * ?")
	public void scheduleFixedDelayTaskadhocNRfetchData() throws MessagingException {

		try {
			URL url = new URL("http://localhost:8080/fetchNRData");
			HttpURLConnection con = (HttpURLConnection) url.openConnection();
			con.setRequestMethod("GET");

			BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
			String inputLine;
			while ((inputLine = in.readLine()) != null) {
				System.out.println(inputLine);
			}

			in.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
	
	//
	// @Scheduled(cron = "0 0/1 * * * ?")
	public void scheduleFixedDelayTaskadhocfetchData() throws MessagingException {

		try {
			URL url = new URL("http://localhost:8080/adhocfetchData");
			HttpURLConnection con = (HttpURLConnection) url.openConnection();
			con.setRequestMethod("GET");

			BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
			String inputLine;
			while ((inputLine = in.readLine()) != null) {
				System.out.println(inputLine);
			}

			in.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	public void sendemail(String type) {

		QrtzEmailConfig mailconfig = (QrtzEmailConfig) osourceTemplate.queryForObject(
				"SELECT *,DATE_FORMAT(NOW(),'%d-%m-%Y') todate,DATE_FORMAT(date(NOW() - INTERVAL 1 DAY),'%d-%m-%Y') fromdate FROM email_config",
				new BeanPropertyRowMapper(QrtzEmailConfig.class));

		// sendEmail(fileNo, hour);

		String toemail = "vijay.uniyal@shubham.co";
		String subject = "Hunter Job "+type+" Running";

		String body = "<html><body><span>Dear Sir/Madam</span><br/><br/><span> Job " + type + mailconfig.getTodate()
				+ " is running <span><br/><br/><span>Regards</span><br/><span>IT Support/IT team</span><body></html>";
		Email sendemail = new Email(toemail, subject, body, null, mailconfig.getSmtphost(), mailconfig.getSmtpport(),
				mailconfig.getUsername(), mailconfig.getPassword());
		Thread emailThread = new Thread(sendemail);
		emailThread.start();

	}
	
	
	//@Scheduled(cron = "0 34 20 * * ?")
	public void scheduleFixedDelayTaskHistory() throws MessagingException {

		try {
			sendemail("scheduleFixedDelayTaskHistory-->fetchDatahistory");
			URL url = new URL("http://localhost:8080/fetchDatahistory");
			HttpURLConnection con = (HttpURLConnection) url.openConnection();
			con.setRequestMethod("GET");

			BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
			String inputLine;
			while ((inputLine = in.readLine()) != null) {
				System.out.println(inputLine);
			}

			in.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		try {
			sendemail("scheduleFixedDelayTaskHistory-->fetchSMEDatahistory");
			URL url = new URL("http://localhost:8080/fetchSMEDatahistory");
			HttpURLConnection con = (HttpURLConnection) url.openConnection();
			con.setRequestMethod("GET");

			BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
			String inputLine;
			while ((inputLine = in.readLine()) != null) {
				System.out.println(inputLine);
			}

			in.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}


}
